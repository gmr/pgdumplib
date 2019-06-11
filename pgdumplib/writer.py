"""
Write pg_dump files using the custom format

"""
import logging
import struct

from pgdumplib import constants

LOGGER = logging.getLogger(__name__)


class Writer:
    """pg_dump custom format compatible writer"""
    def __init__(self, handle):
        self.handle = handle
        self.intsize = None
        self.offsize = None
        self.data_offset = 0

    def write(self, dump):
        """Write the Dump to disk

        :param pgdumplib.dump.Dump dump: The dump instance to write

        """
        self.intsize = dump.toc.header.intsize
        self.offsize = dump.toc.header.offsize

        self._write_header()
        self._write_int(int(dump.toc.compression))
        self._write_timestamp(dump.toc.timestamp)
        self._write_str(dump.toc.dbname)
        self._write_str(dump.toc.server_version)
        self._write_str(dump.toc.dump_version)
        self._write_entries(dump.toc.entries)

    def _write_byte(self, value):
        """Write a byte to the handle

        :param int value: The byte value

        """
        self.handle.write(struct.pack('B', value))

    def _write_entries(self, entries):
        """Write the entries

        :param list entries: The entry list

        """
        self._write_int(len(entries))
        for entry in entries:
            self._write_entry(entry)

    def _write_entry(self, entry):
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

    def _write_header(self):
        """Write the file header"""
        self.handle.write(constants.MAGIC.encode('ASCII'))
        self._write_byte(constants.VERSION[0])
        self._write_byte(constants.VERSION[1])
        self._write_byte(constants.VERSION[2])
        self._write_byte(self.intsize)
        self._write_byte(self.offsize)
        self._write_byte(constants.FORMAT_CUSTOM)

    def _write_int(self, value):
        """Write an integer value

        :param int value:

        """
        self._write_byte(1 if value < 0 else 0)
        if value < 0:
            value = -value
        for offset in range(0, self.intsize):
            self._write_byte(value & 0xFF)
            value >>= 8

    def _write_offset(self, value, data_state):
        """Write the offset value.

        :param int value: The value to write
        :param int data_state: The data state flag

        """
        self._write_byte(data_state)
        for offset in range(0, self.offsize):
            self._write_byte(value & 0xFF)
            value >>= 8

    def _write_str(self, value):
        """Write a string

        :param str value: The string to write

        """
        value = value.encode('utf-8') if value else b''
        self._write_int(len(value))
        if value:
            self.handle.write(value)

    def _write_timestamp(self, value):
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
