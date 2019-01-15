# coding=utf-8
"""
Table of Contents Reader
========================

"""
import collections
import datetime
from os import path
import struct

_MAGIC = 'PGDMP'
_MIN_VER = (1, 12, 0)
_FORMATS = ['Unknown', 'Custom', 'Files', 'Tar', 'Null', 'Directory']
_SECTIONS = ['None', 'Pre-Data', 'Data', 'Post-Data']


Header = collections.namedtuple(
    'Header', ['vmaj', 'vmin', 'vrev', 'intsize', 'offsize', 'format'])

Entry = collections.namedtuple(
    'Entry', ['dump_id', 'had_dumper', 'table_oid', 'oid', 'tag', 'desc',
              'section', 'defn', 'drop_stmt', 'copy_stmt', 'namespace',
              'tablespace', 'owner', 'with_oids', 'dependencies'])


class ToC:
    """Read the table of contents into named tuples."""

    def __init__(self, filename):
        if not path.exists(filename):
            raise OSError('Could not find ToC file: {}'.format(filename))
        self.filename = filename
        self.handle = open(self.filename, 'rb')
        self.header = self._read_header()
        if (self.header.vmaj, self.header.vmin, self.header.vrev) < _MIN_VER:
            raise ValueError(
                'Unsupported backup version: {}.{}.{}'.format(
                    self.header.vmaj, self.header.vmin, self.header.vrev))
        self.compression = self._read_int()
        self.timestamp = self._read_timestamp()
        self.dbname = self._read_bytes().decode('utf-8')
        self.server_version = self._read_bytes().decode('utf-8')
        self.dump_version = self._read_bytes().decode('utf-8')
        self.entries = self._read_entries()

    def _read_byte(self):
        return struct.unpack('B', self.handle.read(1))[0]

    def _read_bytes(self):
        length = self._read_int()
        if length > 0:
            value = self.handle.read(length)
            return value
        return b''

    def _read_dependencies(self):
        values = []
        while True:
            value = self._read_bytes()
            if not value:
                break
            values.append(int(value))
        return values

    def _read_entry(self):
        entry = Entry(
            self._read_int(),
            self._read_int(),
            self._read_bytes().decode('utf-8'),
            self._read_bytes().decode('utf-8'),
            self._read_bytes().decode('utf-8'),
            self._read_bytes().decode('utf-8'),
            _SECTIONS[self._read_int() - 1],
            self._read_bytes().decode('utf-8'),
            self._read_bytes().decode('utf-8'),
            self._read_bytes().decode('utf-8'),
            self._read_bytes().decode('utf-8'),
            self._read_bytes().decode('utf-8'),
            self._read_bytes().decode('utf-8'),
            True if self._read_bytes() == b'true' else False,
            self._read_dependencies())
        offset = self._read_int()  # Offset for data alignment
        if offset:
            self.handle.read(offset)
        return entry

    def _read_entries(self):
        return [self._read_entry() for _i in range(0, self._read_int())]

    def _read_header(self):
        data = self.handle.read(11)
        if struct.unpack_from('5s', data)[0].decode('ASCII') != _MAGIC:
            raise ValueError('Invalid archive header')
        values = {}
        for offset, key in enumerate(Header._fields):
            values[key] = struct.unpack_from('B', data[offset + 5:])[0]
        values['format'] = _FORMATS[values['format']]
        return Header(**values)

    def _read_int(self):
        sign = self._read_byte()
        value = self._read_uint()
        return -value if sign else value

    def _read_uint(self):
        bs, bv, value = 0, 0, 0
        for offset in range(0, self.header.intsize):
            bv = self._read_byte() & 0xff
            if bv != 0:
                value += (bv << bs)
            bs += 8
        return value

    def _read_timestamp(self):
        seconds, minutes, hour, day, month, year, _dst = (
            self._read_int(), self._read_int(), self._read_int(),
            self._read_int(), self._read_int() + 1, self._read_int() + 1900,
            self._read_int())
        return datetime.datetime(year, month, day, hour, minutes, seconds)
