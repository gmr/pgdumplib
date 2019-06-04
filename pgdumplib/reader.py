"""
Common Reader

"""
import dataclasses
import datetime
import io
import logging
import struct

from pgdumplib import constants, exceptions, models

LOGGER = logging.getLogger(__name__)


def _read_byte(handle):
    """Read in an individual byte.

    :rtype: int

    """
    return struct.unpack('B', handle.read(1))[0]


def _read_int(handle, intsize):
    """Read in a signed integer

    :rtype: int

    """
    sign = _read_byte(handle)
    bs, bv, value = 0, 0, 0
    for offset in range(0, intsize):
        bv = _read_byte(handle) & 0xff
        if bv != 0:
            value += (bv << bs)
        bs += 8
    return -value if sign else value


def _skip_data(handle, intsize):
    """Skip data from current file position.
    Data blocks are formatted as an integer length, followed by data.
    A zero length denoted the end of the block.

    :param handle:
    :param int intsize:
    :return:

    """
    block_length, buff_len = _read_int(handle, intsize), 0
    while block_length:
        if block_length > buff_len:
            buff_len = block_length
        data_in = handle.read(block_length)
        if len(data_in) != block_length:
            LOGGER.error('Failure to read full block (%i != %i)',
                         len(data_in), block_length)
            raise ValueError()
        block_length = _read_int(handle, intsize)


class Dump:
    """Dump Object containing data about the dump and includes methods for
    reading data out of the dump.

    """
    def __init__(self, path, toc):
        self.path = path
        self.toc = toc

    def __repr__(self):
        return '<Dump path={!r} format={!r} timestamp={!r}>'.format(
            self.path, self.toc.header.format, self.toc.timestamp.isoformat())

    def read_data(self, namespace, table):
        """Iterator that returns data for the given namespace and table

        :param str namespace: The namespace/schema for the table
        :param str table: The table name
        :raises: :exc:`pgdumplib.exceptions.EntityNotFoundError`

        """
        for entry in [e for e in self.toc.entries if e.section == 'Data']:
            if entry.namespace == namespace and entry.tag == table:
                with open(self.path, 'rb') as handle:
                    for line in self._read_entry_data(entry, handle):
                        yield line
                return
        raise exceptions.EntityNotFoundError(namespace=namespace, table=table)

    def _read_block_header(self, handle):
        """Read the block header in.

        """
        return handle.read(1), _read_int(handle, self.toc.header.intsize)

    def _read_entry_data(self, entry, handle):
        """Read the data from the entry

        :param pgdumplib.models.Entry entry: The entry to read

        """
        if entry.data_state == constants.K_OFFSET_NO_DATA:
            return
        elif entry.data_state == constants.K_OFFSET_POS_NOT_SET:
            block_type, dump_id = self._read_block_header(handle)
            while block_type != constants.EOF and dump_id != entry.dump_id:
                if block_type in [constants.BLK_DATA, constants.BLK_BLOBS]:
                    _skip_data(handle, self.toc.header.intsize)
                else:
                    LOGGER.warning('Unknown block type: %r', block_type)
                block_type, dump_id = self._read_block_header(handle)
        else:
            handle.seek(entry.offset, io.SEEK_SET)

        block_type, dump_id = self._read_block_header(handle)
        if dump_id != entry.dump_id:
            raise ValueError('Dump IDs do not match')

        if block_type == constants.BLK_DATA:
            while True:
                block_length = _read_int(handle, self.toc.header.intsize)
                if block_length == 0:
                    break
                data = handle.read(block_length).decode('utf-8')[:-1]
                if data.startswith('\\.'):
                    break
                yield data.split('\t')
        else:
            raise ValueError(
                'Unsupported block type: {}'.format(block_type))


class ToC:
    """Common base for reading the Table of Contents from a Dump file"""
    def __init__(self, handle):
        """Initialize the ToC Reader, loading in the header

         :param file-object handle: The file handle to read from
         :raises: :exc:`ValueError`

        """
        self.handle = handle
        self.header = self._read_header()
        if self.header.version < constants.MIN_VER:
            raise ValueError(
                'Unsupported backup version: {}.{}.{}'.format(
                    *self.header.version))

    def read(self):
        """Read the Table of Contents from the handle.

        :rtype: pgdumplib.models.ToC

        """
        return models.ToC(
            self.header,
            self._read_int() != 0,               # Compression
            self._read_timestamp(),              # Timestamp
            self._read_bytes().decode('utf-8'),  # dbname
            self._read_bytes().decode('utf-8'),  # Server Name
            self._read_bytes().decode('utf-8'),  # pg_dump Version
            self._read_entries())

    def _read_byte(self):
        """Read in an individual byte.

        :rtype: int

        """
        return struct.unpack('B', self.handle.read(1))[0]

    def _read_bytes(self):
        """Read in a byte stream

        :rtype: bytes

        """
        length = self._read_int()
        if length > 0:
            value = self.handle.read(length)
            return value
        return b''

    def _read_dependencies(self):
        """Read in the dependencies for an entry.

        :rtype: list

        """
        values = []
        while True:
            value = self._read_bytes()
            if not value:
                break
            values.append(int(value))
        return values

    def _read_entry(self):
        """Read in an individual entry.

        :rtype: pgdumplib.models.Entry

        """
        args = [
            self._read_int(),  # Dump ID
            bool(self._read_int()),  # Has Dumper
            self._read_bytes().decode('utf-8'),  # Table OID
            self._read_bytes().decode('utf-8'),  # OID
            self._read_bytes().decode('utf-8'),  # Tag
            self._read_bytes().decode('utf-8'),  # Desc
            constants.SECTIONS[self._read_int() - 1],  # Section
            self._read_bytes().decode('utf-8'),  # Definition
            self._read_bytes().decode('utf-8'),  # Drop Statement
            self._read_bytes().decode('utf-8'),  # Copy Statement
            self._read_bytes().decode('utf-8'),  # Namespace
            self._read_bytes().decode('utf-8'),  # Tablespace
            self._read_bytes().decode('utf-8'),  # Owner
            self._read_bytes() == b'true',  # With OIDs
            self._read_dependencies()
        ]
        data_state, offset = self._read_offset()
        args.append(data_state)
        args.append(offset)
        LOGGER.debug('Args: %r', args)
        return models.Entry(*args)

    def _read_entries(self):
        """Read in all of the entries from the Dump.

        :rtype: list()

        """
        return [self._read_entry() for _i in range(0, self._read_int())]

    def _read_header(self):
        """Read in the dump header

        :rtype: pgdumplib.models.Header
        :raises: ValueError

        """
        data = self.handle.read(11)
        if struct.unpack_from(
                '5s', data)[0].decode('ASCII') != constants.MAGIC:
            raise ValueError('Invalid archive header')
        values = {}
        for offset, field in enumerate(dataclasses.fields(models.Header)):
            values[field.name] = struct.unpack_from('B', data[offset + 5:])[0]
        values['format'] = constants.FORMATS[values['format']]
        return models.Header(**values)

    def _read_int(self):
        """Read in a signed integer

        :rtype: int

        """
        sign = self._read_byte()
        bs, bv, value = 0, 0, 0
        for offset in range(0, self.header.intsize):
            bv = self._read_byte() & 0xff
            if bv != 0:
                value += (bv << bs)
            bs += 8
        return -value if sign else value

    def _read_offset(self):
        """Read in the value for the length of the data stored in the file.

        :rtype: int or None

        """
        if self.header.format != 'Custom':
            return self._read_byte(), None
        data_state = self._read_byte()
        value = 0
        for offset in range(0, self.header.offsize):
            bv = self._read_byte()
            value |= bv << (offset * 8)
        return data_state, value

    def _read_timestamp(self):
        """Read in the timestamp from handle.

        :rtype: datetime.datetime

        """
        seconds, minutes, hour, day, month, year = (
            self._read_int(), self._read_int(), self._read_int(),
            self._read_int(), self._read_int() + 1, self._read_int() + 1900)
        self._read_int()  # DST flag, no way to handle this at the moment
        return datetime.datetime(
            year, month, day, hour, minutes, seconds,
            tzinfo=datetime.timezone.utc)
