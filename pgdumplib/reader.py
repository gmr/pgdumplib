"""
Common Reader

"""
import dataclasses
import datetime
import logging
import struct

from pgdumplib import constants, models

LOGGER = logging.getLogger(__name__)


def read_byte(handle):
    """Read in an individual byte.

    :param file object handle: The file handle to read the integer to
    :rtype: int

    """
    return struct.unpack('B', handle.read(1))[0]


def read_int(handle, intsize):
    """Read in a signed integer

    :param file object handle: The file handle to read the integer to
    :param int intsize: The size of the integer to read
    :rtype: int

    """
    sign = read_byte(handle)
    bs, bv, value = 0, 0, 0
    for offset in range(0, intsize):
        bv = read_byte(handle) & 0xFF
        if bv != 0:
            value += (bv << bs)
        bs += 8
    return -value if sign else value


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
        values = set({})
        while True:
            value = self._read_bytes()
            if not value:
                break
            values.add(int(value))
        return sorted(list(values))

    def _read_entries(self):
        """Read in all of the entries from the Dump.

        :rtype: list()

        """
        entries = self._read_int()
        LOGGER.debug('Reading %i entries', entries)
        return [self._read_entry() for _i in range(0, entries)]

    def _read_entry(self):
        """Read in an individual entry.

        :rtype: pgdumplib.models.Entry

        """
        LOGGER.debug('Reading entry')
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
        return models.Entry(**kwargs)

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
        """Convenience wrapper to :meth:`read_int`.

        :rtype: int

        """
        return read_int(self.handle, self.header.intsize)

    def _read_offset(self):
        """Read in the value for the length of the data stored in the file.

        :rtype: int or None

        """
        data_state = read_byte(self.handle)
        value = 0
        for offset in range(0, self.header.offsize):
            bv = read_byte(self.handle)
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
