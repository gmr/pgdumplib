"""
The :py:class:`~pgdumplib.dump.Dump` class exposes methods to
:py:meth:`load <pgdumplib.dump.Dump.load>` an existing dump,
to :py:meth:`add an entry <pgdumplib.dump.Dump.add_entry>` to a dump,
to :py:meth:`add table data <pgdumplib.dump.Dump.add_data>` to a dump,
to :py:meth:`add blob data <pgdumplib.dump.Dump.add_blob>` to a dump,
and to :py:meth:`save <pgdumplib.dump.Dump.save>` a new dump.

There are :doc:`converters` that are available to format the data that is
returned by :py:meth:`~pgdumplib.dump.Dump.read_data`. The converter
is passed in during construction of a new :py:class:`~pgdumplib.dump.Dump`,
and is also available as an argument to :py:func:`pgdumplib.load`.

The default converter, :py:class:`~pgdumplib.converters.DataConverter` will
return all fields as strings, only replacing ``NULL`` with
:py:const:`None`. The :py:class:`~pgdumplib.converters.SmartDataConverter`
will attempt to convert all columns to native Python data types.

When loading or creating a dump, the table and blob data are stored in
gzip compressed data files in a temporary directory that is automatically
cleaned up when the :py:class:`~pgdumplib.dump.Dump` instance is released.

"""
from __future__ import annotations

import dataclasses
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
from dateutil import tz

from pgdumplib import constants, converters, exceptions, version

LOGGER = logging.getLogger(__name__)

ENCODING_PATTERN = re.compile(r"^.*=\s+'(.*)'")

VERSION_INFO = '{} (pgdumplib {})'.format(constants.APPEAR_AS, version)


class Dump:
    """Create a new instance of the :py:class:`~pgdumplib.dump.Dump` class

    Once created, the instance of :py:class:`~pgdumplib.dump.Dump` can
    be used to read existing dumps or to create new ones.

    :param str dbname: The database name for the dump (Default: ``pgdumplib``)
    :param str encoding: The data encoding (Default: ``UTF8``)
    :param converter: The data converter class to use
        (Default: :py:class:`pgdumplib.converters.DataConverter`)

    """
    def __init__(
            self, dbname: str = 'pgdumplib', encoding: str = 'UTF8',
            converter: typing.Optional[converters.DataConverter] = None):
        self.compression = False
        self.dbname = dbname
        self.dump_version = VERSION_INFO
        self.encoding = encoding
        self.entries = [
            Entry(
                dump_id=1, tag=constants.ENCODING, desc=constants.ENCODING,
                section=constants.SECTION_PRE_DATA,
                defn="SET client_encoding = '{}';".format(self.encoding)),
            Entry(
                dump_id=2, tag='STDSTRINGS', desc='STDSTRINGS',
                section=constants.SECTION_PRE_DATA,
                defn="SET standard_conforming_strings = 'on';"),
            Entry(
                dump_id=3, tag='SEARCHPATH', desc='SEARCHPATH',
                section=constants.SECTION_PRE_DATA,
                defn="SELECT pg_catalog.set_config('search_path', '', false);")
        ]
        self.server_version = VERSION_INFO
        self.timestamp = arrow.now()

        converter = converter or converters.DataConverter
        self._converter = converter()
        self._format = 'Custom'
        self._handle = None
        self._intsize: int = 4
        self._offsize: int = 8
        self._temp_dir = tempfile.TemporaryDirectory()
        self._vmaj: int = constants.MIN_VER[0]
        self._vmin: int = constants.MIN_VER[1]
        self._vrev: int = constants.MIN_VER[2]

    def __repr__(self) -> str:
        return '<Dump format={!r} timestamp={!r} entry_count={!r}>'.format(
            self._format, self.timestamp.isoformat(), len(self.entries))

    def add_entry(
            self,
            namespace: typing.Optional[str],
            tag: str,
            section: str = constants.SECTION_NONE,
            owner: typing.Optional[str] = None,
            desc: typing.Optional[str] = None,
            defn: typing.Optional[str] = None,
            drop_stmt: typing.Optional[str] = None,
            copy_stmt: typing.Optional[str] = None,
            dependencies: typing.Optional[typing.List[int]] = None,
            tablespace: typing.Optional[str] = None,
            dump_id: typing.Optional[int] = None) -> Entry:
        """Add an entry to the dump

        The ``namespace`` and ``tag`` are required.

        A :py:exc:`ValueError` will be raised if section is not one of
        :py:const:`pgdumplib.constants.SECTION_PRE_DATA`,
        :py:const:`pgdumplib.constants.SECTION_DATA`, or
        :py:const:`pgdumplib.constants.SECTION_POST_DATA`.

        When adding data, is is advised to invoke :py:meth:`~Dump.add_data`
        or :py:meth:`~Dump.add_blob` instead of invoking
        :py:meth:`~Dump.add_entry` directly.

        If ``dependencies`` are specified, they will be validated and if a
        ``dump_id`` is specified and no entry is found with that ``dump_id``,
        a :py:exc:`ValueError` will be raised.

        Other omitted values will be set to the default values will be set to
        the defaults specified in the :py:class:`pgdumplib.dump.Entry`
        class.

        The ``dump_id`` will be auto-calculated based upon the existing entries
        if it is not specified.

        :param str namespace: The namespace of the entry
        :param str tag: The name/table/relation/etc of the entry
        :param str section: The section for the entry
        :param str owner: The owner of the object in Postgres
        :param str desc: The entry description
        :param str defn: The DDL definition for the entry
        :param drop_stmt: A drop statement used to drop the entry before
        :param copy_stmt: A copy statement used when there is a corresponding
            data section.
        :param list dependencies: A list of dump_ids of objects that the entry
            is dependent upon.
        :param str tablespace: The tablespace to use
        :param int dump_id: The dump id, will be auto-calculated if left empty
        :raises: :py:exc:`ValueError`
        :rtype: pgdumplib.dump.Entry

        """
        if section not in constants.SECTIONS:
            raise ValueError('Invalid section: {}'.format(section))

        dump_ids = [e.dump_id for e in self.entries]

        for dependency in dependencies or []:
            if dependency not in dump_ids:
                raise ValueError(
                    'Dependency dump_id {!r} not found'.format(dependency))

        if not dump_id:
            dump_id = max(dump_ids) + 1

        self.entries.append(Entry(
            dump_id, False, None, None, tag, desc, section, defn, drop_stmt,
            copy_stmt, namespace, tablespace, owner, False, dependencies))

        LOGGER.debug('Appended entry #%i: %r',
                     len(self.entries), self.entries[-1])
        return self.entries[-1]

    def get_entry(self,
                  namespace: str, tag: str,
                  section: str = constants.SECTION_PRE_DATA) \
            -> typing.Optional[Entry]:
        """Return the entry for the given namespace and tag

        :param str namespace: The namespace of the entry
        :param str tag: The tag/relation/table name
        :param str section: The dump section the entry is for
        :raises: :py:exc:`ValueError`
        :rtype: pgdumplib.dump.Entry or None

        """
        if section not in constants.SECTIONS:
            raise ValueError('Invalid section: {}'.format(section))
        for entry in [e for e in self.entries if e.section == section]:
            if entry.namespace == namespace and entry.tag == tag:
                return entry

    def get_entry_by_dump_id(self, dump_id: int) \
            -> typing.Optional[Entry]:
        """Return the entry for the given `dump_id`

        :param int dump_id: The dump ID of the entry to return.
        :rtype: pgdumplib.dump.Entry or None

        """
        for entry in self.entries:
            if entry.dump_id == dump_id:
                return entry

    def read_data(self, namespace: str, table: str) -> tuple:
        """Iterator that returns data for the given namespace and table

        :param str namespace: The namespace/schema for the table
        :param str table: The table name
        :raises: :py:exc:`pgdumplib.exceptions.EntityNotFoundError`

        """
        for entry in self._data_entries:
            if entry.namespace == namespace and entry.tag == table:
                for row in self._read_table_data(entry):
                    yield self._converter.convert(row)
                return
        raise exceptions.EntityNotFoundError(namespace=namespace, table=table)

    def load(self, path: str) -> Dump:
        """Load the Dumpfile, including extracting all data into a temporary
        directory

        :raises: :py:exc:`ValueError`

        """
        if not pathlib.Path(path).exists():
            raise ValueError('Path {!r} does not exist'.format(path))

        self.entries = []  # Wipe out pre-existing entries
        self._handle = open(path, 'rb')
        self._read_header()
        if not constants.MIN_VER <= self.version <= constants.MAX_VER:
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
            with gzip.open(path, 'wb') as handle:
                if entry.desc == constants.BLOBS:
                    for oid, blob in self._read_entry_data(entry):
                        handle.write(struct.pack('I', oid))
                        handle.write(struct.pack('I', len(blob)))
                        handle.write(blob)
                else:
                    data = self._read_entry_data(entry)
                    if data:
                        handle.write(data)

        return self

    def save(self, path: str = None) -> None:
        """Save the Dump file to the specified path

        :param str path: The path to save the dump to

        """
        if self._handle:
            self._handle.close()
        self.compression = False
        self._handle = open(path, 'wb')
        self._save()
        self._handle.close()
        self.load(path)

    @property
    def version(self) -> typing.Tuple[int, int, int]:
        """Return the version as a tuple to make version comparisons easier.

        :rtype: tuple

        """
        return self._vmaj, self._vmin, self._vrev

    @property
    def _data_entries(self) -> typing.List[Entry]:
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
            buffer = self._read_data()
            yield oid, buffer
            oid = self._read_int()

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

    def _read_entry_data(self, entry) -> typing.Optional[None, bytes, tuple]:
        """Read the data from the entry

        :param pgdumplib.dump.Entry entry: The entry to read
        :raises: :exc:`RuntimeError`

        """
        LOGGER.debug('Reading entry data for %i %s %s (%s) @ %i',
                     entry.dump_id, entry.namespace, entry.tag, entry.desc,
                     entry.offset)
        if entry.data_state == constants.K_OFFSET_NO_DATA:
            return b''
        elif entry.data_state != constants.K_OFFSET_POS_SET:
            raise RuntimeError('Unsupported data format')
        self._handle.seek(entry.offset, io.SEEK_SET)
        block_type, dump_id = self._read_block_header()
        if dump_id != entry.dump_id:
            raise RuntimeError('Dump IDs do not match ({} != {}'.format(
                dump_id, entry.dump_id))
        if block_type == constants.BLK_DATA:
            return self._read_data()
        elif block_type == constants.BLK_BLOBS:
            return self._read_blobs()
        else:
            raise RuntimeError('Unknown block type: {}'.format(block_type))

    def _read_entries(self) -> None:
        """Read in all of the entries"""
        entries = self._read_int()
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
        self.entries.append(Entry(**kwargs))

    def _read_header(self) -> None:
        """Read in the dump header

        :raises: ValueError

        """
        if self._handle.read(5) != constants.MAGIC:
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

    def _read_table_data(self, entry) -> str:
        """Iterate through the data returning on row at a time

        :rtype: str

        """
        data = self._read_entry_data(entry).decode(self.encoding).split('\n')
        if len(data) == 1 and not data[0]:
            data = []
        for line in data:
            if line.startswith('\\.') or not line:
                break
            yield line

    def _read_timestamp(self) -> datetime.datetime:
        """Read in the timestamp from handle.

        :rtype: datetime.datetime

        """
        second, minute, hour, day, month, year = (
            self._read_int(), self._read_int(), self._read_int(),
            self._read_int(), self._read_int() + 1, self._read_int() + 1900)
        self._read_int()  # DST flag
        return arrow.Arrow(
            year, month, day, hour, minute, second, 0, tz.tzlocal()).datetime

    def _save(self) -> None:
        """Save the dump file to disk"""
        self._write_toc()
        self._write_data()
        self._write_toc()

    def _set_encoding(self) -> None:
        """If the encoding is found in the dump entries, set the encoding
        to `self.encoding`.

        """
        for entry in self.entries:
            if entry.desc == constants.ENCODING:
                LOGGER.debug('Matched on encoding')
                match = ENCODING_PATTERN.match(entry.defn)
                self.encoding = match.group(1)
                return

    def _write_byte(self, value) -> None:
        """Write a byte to the handle

        :param int value: The byte value

        """
        self._handle.write(struct.pack('B', value))

    def _write_data(self):
        """Write the data blocks"""
        for offset, entry in enumerate(self.entries):
            if entry.section != constants.SECTION_DATA:
                continue

            self.entries[offset].data_state = constants.K_OFFSET_POS_SET
            self.entries[offset].offset = self._handle.tell()

            path = pathlib.Path(self._temp_dir.name) / '{}.gz'.format(
                entry.dump_id)
            with gzip.open(path, 'rb') as handle:
                if entry.desc == constants.TABLE_DATA:
                    self._handle.write(constants.BLK_DATA)
                    self._write_int(entry.dump_id)

                    # Get the total size
                    handle.seek(0, io.SEEK_END)
                    size = handle.tell()
                    if not size:
                        self.entries[offset].data_state = \
                            constants.K_OFFSET_NO_DATA
                    self._write_int(size)

                    # Go back to start of file
                    handle.seek(0)
                    if size:
                        self._handle.write(handle.read(size))
                    self._write_int(0)

                elif entry.desc == constants.BLOBS:
                    self._handle.write(constants.BLK_BLOBS)
                    self._write_int(entry.dump_id)
                    while True:
                        try:
                            oid = struct.unpack('I', handle.read(4))[0]
                        except struct.error:
                            break
                        length = struct.unpack('I', handle.read(4))[0]
                        self._write_int(oid)
                        self._write_int(length)
                        self._handle.write(handle.read(length))
                        self._write_int(0)
                    self._write_int(0)
                else:
                    raise ValueError(
                        'Unknown block type: {}'.format(entry.desc))

    def _write_entries(self) -> None:
        """Write the toc entries"""
        LOGGER.debug('Writing %i entries', len(self.entries))
        self._write_int(len(self.entries))
        for entry in self.entries:
            self._write_entry(entry)

    def _write_entry(self, entry) -> None:
        """Write the entry

        :param pgdumplib.dump.Entry entry:

        """
        LOGGER.debug(
            'Writing %i %s %s %s %r',
            entry.dump_id, entry.desc, entry.namespace, entry.tag, entry.defn)
        self._write_int(entry.dump_id)
        self._write_int(int(entry.had_dumper))
        self._write_str(entry.table_oid or '0')
        self._write_str(entry.oid or '0')
        self._write_str(entry.tag)
        self._write_str(entry.desc)
        self._write_int(constants.SECTIONS.index(entry.section) + 1)
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
        self._write_offset(entry.offset, entry.data_state)

    def _write_header(self) -> None:
        """Write the file header"""
        self._handle.write(constants.MAGIC)
        self._write_byte(self._vmaj)
        self._write_byte(self._vmin)
        self._write_byte(self._vrev)
        self._write_byte(self._intsize)
        self._write_byte(self._offsize)
        self._write_byte(constants.FORMATS.index(self._format))

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

    def _write_toc(self) -> None:
        """Write the ToC for the file"""
        self._handle.seek(0)
        self._write_header()
        self._write_int(int(self.compression))
        self._write_timestamp(self.timestamp)
        self._write_str(self.dbname)
        self._write_str(self.server_version)
        self._write_str(self.dump_version)
        self._write_entries()


@dataclasses.dataclass
class Entry:
    """The entry model represents a single entry in the dataclass

    Custom formatted dump files are primarily comprised of entries, which
    contain all of the metadata and DDL required to construct the database.

    For table data and blobs, there are entries that contain offset locations
    in the dump file that instruct the reader as to where the data lives
    in the file.

    :var int dump_id: The dump id, will be auto-calculated if left empty
    :var bool had_dumper: Indicates
    :var str oid: The OID of the object the entry represents
    :var str tag: The name/table/relation/etc of the entry
    :var str desc: The entry description
    :var str section: The section for the entry
    :var str defn: The DDL definition for the entry
    :var str drop_stmt: A drop statement used to drop the entry before
    :var str copy_stmt: A copy statement used when there is a corresponding
        data section.
    :var str namespace: The namespace of the entry
    :var str tablespace: The tablespace to use
    :var str owner: The owner of the object in Postgres
    :var bool with_oids: Indicates ...
    :var list dependencies: A list of dump_ids of objects that the entry
        is dependent upon.
    :var int data_state: Indicates if the entry has data and how it is stored
    :var int offset: If the entry has data, the offset to the data in the file

    """
    dump_id: int
    had_dumper: bool = False
    table_oid: typing.Optional[str] = None
    oid: typing.Optional[str] = None
    tag: typing.Optional[str] = None
    desc: typing.Optional[str] = None
    section: str = constants.SECTIONS[0]
    defn: typing.Optional[str] = None
    drop_stmt: typing.Optional[str] = None
    copy_stmt: typing.Optional[str] = None
    namespace: typing.Optional[str] = None
    tablespace: typing.Optional[str] = None
    owner: typing.Optional[str] = None
    with_oids: bool = False
    dependencies: list = dataclasses.field(default_factory=list)
    data_state: int = constants.K_OFFSET_NO_DATA
    offset: int = 0
