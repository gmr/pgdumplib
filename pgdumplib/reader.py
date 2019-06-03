"""
Common Reader

"""
import dataclasses
import datetime
import io
import logging
import struct

from pgdumplib import constants, models

LOGGER = logging.getLogger(__name__)


class Reader:
    """Common base for reading in a pg_dump file"""
    def __init__(self, handle):
        """Initialize the Reader, loading in the header

         :param file-object handle: The file handle to read from
         :raises: :exc:`ValueError`

        """
        self.handle = handle
        self.header = self._read_header()
        if self.header.version < constants.MIN_VER:
            raise ValueError(
                'Unsupported backup version: {}.{}.{}'.format(
                    *self.header.version))

    def read_toc(self):
        """Read the Table of Contents from the handle.

        :rtype: pgdumplib.models.ToC

        """
        return models.ToC(
            self.header,
            self._read_int() != 0,  # Compression
            self._read_timestamp(),
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
            self._read_dependencies(),  # Dependencies
            self.handle.tell(),   # Current Position
            self._read_int()  # Number of bytes with data
        ]

        if args[-1] > 0:
            self.handle.seek(args[-1], io.SEEK_CUR)

        # Extra field defined in custom format
        args.append(self._read_uint() if self.header.format == 'Custom'
                    else None)

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
        value = self._read_uint()
        return -value if sign else value
    
    def _read_uint(self):
        """Read in an unsigned integer

        :rtype: int

        """
        bs, bv, value = 0, 0, 0
        for offset in range(0, self.header.intsize):
            bv = self._read_byte() & 0xff
            if bv != 0:
                value += (bv << bs)
            bs += 8
        return value
    
    def _read_timestamp(self):
        """Read in the timestamp from handle.

        :rtype: datetime.datetime

        """
        seconds, minutes, hour, day, month, year, _dst = (
            self._read_int(), self._read_int(), self._read_int(),
            self._read_int(), self._read_int() + 1, self._read_int() + 1900,
            self._read_int())
        return datetime.datetime(
            year, month, day, hour, minutes, seconds,
            tzinfo=datetime.timezone.utc)
