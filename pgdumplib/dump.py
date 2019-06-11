"""
Class representing a pg_dump

"""
from __future__ import annotations

import datetime
import gzip
import io
import logging
import pathlib
import re
import struct
import tempfile
import typing
import zlib

import arrow

from pgdumplib import constants, converters, exceptions, models, version

LOGGER = logging.getLogger(__name__)

ENCODING_PATTERN = re.compile(r"^SET\s+client_encoding\s+=\s+'(.*)';")

VERSION_INFO = '{} (pgdumplib {})'.format(constants.APPEAR_AS, version)


class Dump:
    """Dump Object containing data about the dump and includes methods for
    reading data out of the dump.

    """
    def __init__(self, dbname='pgdumplib', encoding='UTF8',
                 converter=converters.DataConverter):
        self.compression = False
        self.dbname = dbname
        self.dump_version = VERSION_INFO
        self.encoding = encoding
        self.entries = []
        self.server_version = VERSION_INFO
        self.timestamp = arrow.now()

        self._converter = converter()
        self._format = constants.FORMAT_CUSTOM
        self._handle = None
        self._intsize: int = 4
        self._offsize: int = 8
        self._temp_dir = tempfile.TemporaryDirectory()
        self._vmaj: int = constants.MIN_VER[0]
        self._vmin: int = constants.MIN_VER[1]
        self._vrev: int = constants.MIN_VER[2]

    def __repr__(self):
        return '<Dump format={!r} timestamp={!r} entry_count={!r}>'.format(
            self._format, self.timestamp.isoformat(), len(self.entries))

    def add_entry(self, namespace=None, tag=None,
                  section=constants.SECTION_NONE, owner=None, desc=None,
                  tablespace=None, defn=None, drop_stmt=None, copy_stmt=None,
                  dependencies=None, dump_id=None) -> models.Entry:
        """Add an entry to the dump

        The `namespace`, `tag`, and `section` are required.

        A :exc:`ValueError` will be raised if section is not one of
        :const:`~pgdumplib.constants.SECTION_NONE`,
        :const:`~pgdumplib.constants.SECTION_PRE_DATA`,
        :const:`~pgdumplib.constants.SECTION_DATA`, or
        :const:`~pgdumplib.constants.SECTION_POST_DATA`.

        When adding data, is is advised to invoke :meth:`~Dump.add_data`
        instead of invoking :meth:`~Dump.add_entry` directly.

        If `dependencies` are specified, they will be validated and if a
        `dump_id` is specified and no entry is found with that `dump_id`,
        a :exc:`ValueError` will be raised.

        Other omitted values will be set to the default values will be set to
        the defaults specified in the :class:`~pgdumplib.models.Entry` class.

        The `dump_id` will be auto-calculated based upon the existing entries
        if it is not specified.

        :param str namespace: The namespace of the entry
        :param str tag: The name/table/relation/etc of the entry
        :param str section: The section for the entry
        :param str owner: The owner of the object in Postgres
        :param str desc: The entry description
        :param str tablespace: The tablespace to use
        :param str defn: The DDL definition for the entry
        :param drop_stmt: A drop statement used to drop the entry before
        :param copy_stmt: A copy statement used when there is a corresponding
            data section.
        :param list dependencies: A list of dump_ids of objects that the entry
            is dependent upon.
        :param int dump_id: The dump id, will be auto-calculated if left empty
        :raises: ValueError
        :rtype: pgdumplib.models.Entry

        """
        if section not in constants.SECTIONS:
            raise ValueError('Invalid section: {}'.format(section))

        dump_ids = [e.dump_id for e in self.entries]

        for dependency in dependencies or []:
            if dependency not in dump_ids:
                raise ValueError(
                    'Dependency dump_id {!r} not found'.format(dependency))

        if not dump_id:
            dump_id = max(dump_ids) + 1 if dump_ids else 1

        self.entries.append(models.Entry(
            dump_id, False, None, None, tag, desc, section, defn, drop_stmt,
            copy_stmt, namespace, tablespace, owner, False, dependencies))
        return self.entries[-1]

    def get_entry(self, namespace, tag, section=constants.SECTION_PRE_DATA) \
            -> typing.Optional[models.Entry]:
        """Return the entry for the given namespace and tag

        :param str namespace: The namespace of the entry
        :param str tag: The tag/relation/table name
        :param str section: The dump section the entry is for
        :raises: ValueError
        :rtype: pgdumplib.models.Entry or None

        """
        if section not in constants.SECTIONS:
            raise ValueError('Invalid section: {}'.format(section))
        for entry in [e for e in self.entries if e.section == section]:
            if entry.namespace == namespace and entry.tag == tag:
                return entry

    def get_entry_by_dump_id(self, dump_id) -> typing.Optional[models.Entry]:
        """Return the entry for the given `dump_id`

        :param int dump_id: The dump ID of the entry to return.
        :rtype: pgdumplib.models.Entry or None

        """
        for entry in self.entries:
            if entry.dump_id == dump_id:
                return entry

    def read_data(self, namespace, table) -> tuple:
        """Iterator that returns data for the given namespace and table

        :param str namespace: The namespace/schema for the table
        :param str table: The table name
        :raises: :exc:`pgdumplib.exceptions.EntityNotFoundError`

        """
        for entry in self._data_entries:
            if entry.namespace == namespace and entry.tag == table:
                for row in self._read_entry_data(entry):
                    yield self._converter.convert(row)
                return
        raise exceptions.EntityNotFoundError(namespace=namespace, table=table)

    def load(self, path) -> Dump:
        """Load the Dumpfile, including extracting all data into a temporary
        directory

        :raises: :exc:`ValueError`

        """
        if not pathlib.Path(path).exists():
            raise ValueError('Path {!r} does not exist'.format(path))

        self._handle = open(path, 'rb')

        self._read_header()

        if self.version < constants.MIN_VER:
            raise ValueError(
                'Unsupported backup version: {}.{}.{}'.format(
                    *self.version))

        self.compression = self._read_int() != 0
        self.timestamp = self._read_timestamp()
        self.dbname = self._read_bytes().decode(self.encoding)
        self.server_version = self._read_bytes().decode(self.encoding)
        self.dump_version = self._read_bytes().decode(self.encoding)

        self._read_entries()
        self._set_encoding()

        # Write out data entries
        for entry in self._data_entries:
            path = pathlib.Path(self._temp_dir.name) / '{}.gz'.format(
                entry.dump_id)
            LOGGER.debug('Writing %s.%s (%i) to %s',
                         entry.namespace, entry.tag, entry.dump_id, path)
            with gzip.open(path, 'wb') as handle:
                if entry.desc == constants.BLOBS:
                    for oid, blob in self._read_entry_data(entry):
                        handle.write(struct.pack('I', oid))
                        handle.write(struct.pack('I', len(blob)))
                        handle.write(blob)
                else:
                    for line in self._read_entry_data(entry):
                        handle.write(line.encode(self.encoding) + b'\n')

        return self

    def save(self, path=None) -> None:
        """Save the Dump file to the specified path

        :param str path: The path to save the dump to

        """
        if self._handle:
            self._handle.close()
        self._handle = open(path, 'wb')
        self._save()
        self._handle.close()
        self.load(path)

    @property
    def version(self):
        """Return the version as a tuple to make version comparisons easier.

        :rtype: tuple

        """
        return self._vmaj, self._vmin, self._vrev

    @property
    def _data_entries(self) -> list:
        """Return the list of entries that are in the data section

        :rtype: list

        """
        return [e for e in self.entries if e.section == constants.SECTION_DATA]

    def _read_blobs(self) -> (int, bytes):
        """Read blobs, returning a tuple of the blob ID and the blob data

        :rtype: (int, bytes)

        """
        oid = self._read_int()
        while oid:
            LOGGER.debug('Reading blob %s @ %i', oid, self._handle.tell())
            buffer = self._read_data()
            yield oid, buffer
            oid = self._read_int()
            LOGGER.info('Next OID: %r', oid)

    def _read_block_header(self) -> (bytes, int):
        """Read the block header in

        :rtype: bytes, int

        """
        return self._handle.read(1), self._read_int()

    def _read_byte(self) -> int:
        """Read in an individual byte

        :rtype: int

        """
        return struct.unpack('B', self._handle.read(1))[0]

    def _read_bytes(self) -> bytes:
        """Read in a byte stream

        :rtype: bytes

        """
        length = self._read_int()
        if length > 0:
            value = self._handle.read(length)
            return value
        return b''

    def _read_data(self) -> bytes:
        """Read a data block, returning the bytes.

        :rtype: bytes

        """
        if self.compression:
            return self._read_data_compressed()
        return self._read_data_uncompressed()

    def _read_data_compressed(self) -> bytes:
        """Read a compressed data block

        :rtype: bytes

        """
        buffer = io.BytesIO()
        chunk = b''
        decompress = zlib.decompressobj()
        while True:
            chunk_size = self._read_int()
            chunk += self._handle.read(chunk_size)
            buffer.write(decompress.decompress(chunk))
            chunk = decompress.unconsumed_tail
            if not chunk and chunk_size < constants.ZLIB_IN_SIZE:
                break
        return buffer.getvalue()

    def _read_data_uncompressed(self) -> bytes:
        """Read an uncompressed data block

        :rtype: bytes

        """
        buffer = io.BytesIO()
        while True:
            block_length = self._read_int()
            if block_length <= 0:
                break
            buffer.write(self._handle.read(block_length))
        return buffer.getvalue()

    def _read_dependencies(self) -> list:
        """Read in the dependencies for an entry.

        :rtype: list

        """
        values = set({})
        while True:
            value = self._read_bytes()
            if not value:
                break
            values.add(int(value))
        return sorted(list(values))

    def _read_entry_data(self, entry) -> str:
        """Read the data from the entry

        :param pgdumplib.models.Entry entry: The entry to read
        :raises: :exc:`ValueError`

        """
        if entry.data_state == constants.K_OFFSET_NO_DATA:
            LOGGER.debug('K_OFFSET_NO_DATA')
            return

        elif entry.data_state == constants.K_OFFSET_POS_NOT_SET:
            block_type, dump_id = self._read_block_header()
            LOGGER.debug('K_OFFSET_POS_NOT_SET - %r, %r', block_type, dump_id)
            while block_type != constants.EOF and dump_id != entry.dump_id:
                if block_type not in [constants.BLK_DATA, constants.BLK_BLOBS]:
                    raise ValueError(
                        'Unknown block type: {}'.format(block_type))
                self._skip_data()
                block_type, dump_id = self._read_block_header()
                LOGGER.debug('K_OFFSET_POS_NOT_SET - %r, %r',
                             block_type, dump_id)
        else:
            LOGGER.debug('K_OFFSET_POS_SET')

            self._handle.seek(entry.offset, io.SEEK_SET)
            block_type, dump_id = self._read_block_header()
            if dump_id != entry.dump_id:
                raise ValueError('Dump IDs do not match')

        if block_type == constants.BLK_DATA:
            for line in self._read_table_data():
                yield line
        elif block_type == constants.BLK_BLOBS:
            for blob in self._read_blobs():
                yield blob
        else:
            raise ValueError('Unknown block type: {}'.format(block_type))

    def _read_entries(self) -> None:
        """Read in all of the entries"""
        entries = self._read_int()
        LOGGER.debug('Reading %i entries', entries)
        [self._read_entry() for _i in range(0, entries)]

    def _read_entry(self) -> None:
        """Read in an individual entry and append it to the entries stack"""
        kwargs = {
            'dump_id': self._read_int(),
            'had_dumper': bool(self._read_int()),
            'table_oid': self._read_bytes().decode('utf-8') or None,
            'oid': self._read_bytes().decode('utf-8') or None,
            'tag': self._read_bytes().decode('utf-8') or None,
            'desc': self._read_bytes().decode('utf-8') or None,
            'section': constants.SECTIONS[self._read_int() - 1],
            'defn': self._read_bytes().decode('utf-8') or None,
            'drop_stmt': self._read_bytes().decode('utf-8') or None,
            'copy_stmt': self._read_bytes().decode('utf-8') or None,
            'namespace': self._read_bytes().decode('utf-8') or None,
            'tablespace': self._read_bytes().decode('utf-8') or None,
            'owner': self._read_bytes().decode('utf-8') or None,
            'with_oids': self._read_bytes() == b'true',
            'dependencies': self._read_dependencies()
        }
        kwargs['data_state'], kwargs['offset'] = self._read_offset()
        self.entries.append(models.Entry(**kwargs))

    def _read_header(self) -> None:
        """Read in the dump header

        :raises: ValueError

        """
        if self._handle.read(5).decode('ASCII') != constants.MAGIC:
            raise ValueError('Invalid archive header')

        self._vmaj = struct.unpack('B', self._handle.read(1))[0]
        self._vmin = struct.unpack('B', self._handle.read(1))[0]
        self._vrev = struct.unpack('B', self._handle.read(1))[0]
        self._intsize = struct.unpack('B', self._handle.read(1))[0]
        self._offsize = struct.unpack('B', self._handle.read(1))[0]
        self._format = constants.FORMATS[struct.unpack(
            'B', self._handle.read(1))[0]]

    def _read_int(self) -> int:
        """Read in a signed integer

        :rtype: int

        """
        sign = self._read_byte()
        bs, bv, value = 0, 0, 0
        for offset in range(0, self._intsize):
            bv = self._read_byte() & 0xFF
            if bv != 0:
                value += (bv << bs)
            bs += 8
        return -value if sign else value

    def _read_offset(self) -> (int, int):
        """Read in the value for the length of the data stored in the file

        :rtype: int, int

        """
        data_state = self._read_byte()
        value = 0
        for offset in range(0, self._offsize):
            bv = self._read_byte()
            value |= bv << (offset * 8)
        return data_state, value

    def _read_table_data(self) -> str:
        """Iterate through the data returning on row at a time

        :rtype: str

        """
        for line in self._read_data().decode(self.encoding).split('\n'):
            if line.startswith('\\.'):
                break
            yield line

    def _read_timestamp(self) -> datetime.datetime:
        """Read in the timestamp from handle.

        :rtype: datetime.datetime

        """
        seconds, minutes, hour, day, month, year = (
            self._read_int(), self._read_int(), self._read_int(),
            self._read_int(), self._read_int() + 1, self._read_int() + 1900)
        self._read_int()  # DST flag
        return arrow.Arrow(
            year, month, day, hour, minutes, seconds).to('local').datetime

    def _save(self) -> None:
        """Save the dump file to disk"""
        self._write_header()
        self._write_int(int(self.compression))
        self._write_timestamp(self.timestamp)
        self._write_str(self.dbname)
        self._write_str(self.server_version)
        self._write_str(self.dump_version)
        self._write_entries()

    def _set_encoding(self) -> None:
        """If the encoding is found in the dump entries, set the encoding
        to `self.encoding`.

        """
        for entry in self.entries:
            if entry.desc == 'ENCODING':
                match = ENCODING_PATTERN.match(entry.defn)
                self.encoding = match.group(1)
                LOGGER.debug('Set encoding to %s', self.encoding)
                return

    def _skip_data(self) -> None:
        """Skip data from current file position.
        Data blocks are formatted as an integer length, followed by data.
        A zero length denoted the end of the block.

        """
        block_length, buff_len = self._read_int(), 0
        LOGGER.debug('Skipping %i', block_length)
        while block_length:
            if block_length > buff_len:
                buff_len = block_length
            data_in = self._handle.read(block_length)
            if len(data_in) != block_length:
                LOGGER.error('Failure to read full block (%i != %i)',
                             len(data_in), block_length)
                raise ValueError('Skip Read Failure')
            block_length = self._read_int()

    def _write_byte(self, value) -> None:
        """Write a byte to the handle

        :param int value: The byte value

        """
        self._handle.write(struct.pack('B', value))

    def _write_entries(self) -> None:
        """Write the entries"""
        self._write_int(len(self.entries))
        for entry in self.entries:
            self._write_entry(entry)

    def _write_entry(self, entry) -> None:
        """Write the entry

        :param pgdumplib.models.Entry entry:

        """
        self._write_int(entry.dump_id)
        self._write_int(int(entry.had_dumper))
        self._write_str(entry.table_oid or '0')
        self._write_str(entry.oid or '0')
        self._write_str(entry.tag)
        self._write_str(entry.desc)
        self._write_int(constants.SECTIONS.index(entry.section))
        self._write_str(entry.defn)
        self._write_str(entry.drop_stmt)
        self._write_str(entry.copy_stmt)
        self._write_str(entry.namespace)
        self._write_str(entry.tablespace)
        self._write_str(entry.owner)
        self._write_str('true' if entry.with_oids else 'false')
        for dependency in entry.dependencies or []:
            self._write_str(str(dependency))
        self._write_int(-1)
        self._write_offset(0, entry.data_state)

    def _write_header(self) -> None:
        """Write the file header"""
        self._handle.write(constants.MAGIC.encode('ASCII'))
        self._write_byte(self._vmaj)
        self._write_byte(self._vmaj)
        self._write_byte(self._vmaj)
        self._write_byte(self._intsize)
        self._write_byte(self._offsize)
        self._write_byte(self._format)

    def _write_int(self, value) -> None:
        """Write an integer value

        :param int value:

        """
        self._write_byte(1 if value < 0 else 0)
        if value < 0:
            value = -value
        for offset in range(0, self._intsize):
            self._write_byte(value & 0xFF)
            value >>= 8

    def _write_offset(self, value, data_state) -> None:
        """Write the offset value.

        :param int value: The value to write
        :param int data_state: The data state flag

        """
        self._write_byte(data_state)
        for offset in range(0, self._offsize):
            self._write_byte(value & 0xFF)
            value >>= 8

    def _write_str(self, value) -> None:
        """Write a string

        :param str value: The string to write

        """
        value = value.encode(self.encoding) if value else b''
        self._write_int(len(value))
        if value:
            self._handle.write(value)

    def _write_timestamp(self, value) -> None:
        """Write a datetime.datetime value

        :param datetime.datetime value: The value to write

        """
        self._write_int(value.second)
        self._write_int(value.minute)
        self._write_int(value.hour)
        self._write_int(value.day)
        self._write_int(value.month - 1)
        self._write_int(value.year - 1900)
        self._write_int(1 if value.dst() else 0)
