import gzip
import logging
import pathlib
import struct
import tempfile
import unittest
from unittest import mock

import pgdumplib
from pgdumplib import constants, dump

LOGGER = logging.getLogger(__name__)


class EdgeTestCase(unittest.TestCase):
    @staticmethod
    def _write_byte(handle, value) -> None:
        """Write a byte to the handle"""
        handle.write(struct.pack('B', value))

    def _write_int(self, handle, value):
        self._write_byte(handle, 1 if value < 0 else 0)
        if value < 0:
            value = -value
        for _offset in range(0, 4):
            self._write_byte(handle, value & 0xFF)
            value >>= 8

    def tearDown(self) -> None:
        test_file = pathlib.Path('build/data/dump.test')
        if test_file.exists():
            test_file.unlink()

    def test_invalid_dependency(self):
        dmp = pgdumplib.new('test')
        with self.assertRaises(ValueError):
            dmp.add_entry(
                constants.TABLE, '', 'block_table', dependencies=[1024]
            )

    def test_invalid_block_type_in_data(self):
        dmp = pgdumplib.new('test')
        dmp.add_entry(constants.TABLE_DATA, '', 'block_table', dump_id=128)
        with gzip.open(pathlib.Path(dmp._temp_dir.name) / '128.gz', 'wb') as h:
            h.write(b'1\t\1\t\1\n')
        with mock.patch('pgdumplib.constants.BLK_DATA', b'\x02'):
            dmp.save('build/data/dump.test')
        with self.assertRaises(RuntimeError):
            pgdumplib.load('build/data/dump.test')

    def test_encoding_not_first_entry(self):
        dmp = pgdumplib.new('test', 'LATIN1')
        entries = dmp.entries
        dmp.entries = [entries[1], entries[2], entries[0]]
        self.assertEqual(dmp.encoding, 'LATIN1')
        dmp.save('build/data/dump.test')

        dmp = pgdumplib.load('build/data/dump.test')
        self.assertEqual(dmp.encoding, 'LATIN1')

    def test_encoding_no_entries(self):
        dmp = pgdumplib.new('test', 'LATIN1')
        dmp.entries = []
        self.assertEqual(dmp.encoding, 'LATIN1')
        dmp.save('build/data/dump.test')
        dmp = pgdumplib.load('build/data/dump.test')
        self.assertEqual(dmp.encoding, 'UTF8')

    def test_dump_id_mismatch_in_data(self):
        dmp = pgdumplib.new('test')
        dmp.add_entry(constants.TABLE_DATA, '', 'block_table', dump_id=1024)
        with gzip.open(
            pathlib.Path(dmp._temp_dir.name) / '1024.gz', 'wb'
        ) as handle:
            handle.write(b'1\t\1\t\1\n')
        dmp.save('build/data/dump.test')

        with mock.patch('pgdumplib.dump.Dump._read_block_header') as rbh:
            rbh.return_value = constants.BLK_DATA, 2048
            with self.assertRaises(RuntimeError):
                pgdumplib.load('build/data/dump.test')

    def test_no_data(self):
        dmp = pgdumplib.new('test')
        dmp.add_entry(constants.TABLE_DATA, '', 'empty_table', dump_id=5)
        with gzip.open(pathlib.Path(dmp._temp_dir.name) / '5.gz', 'wb') as h:
            h.write(b'')
        dmp.save('build/data/dump.test')
        dmp = pgdumplib.load('build/data/dump.test')
        data = list(dmp.table_data('', 'empty_table'))
        self.assertEqual(len(data), 0)

    def test_runtime_error_when_pos_not_set(self):
        dmp = pgdumplib.new('test')
        dmp.add_entry(constants.TABLE_DATA, 'public', 'table', dump_id=32)
        with gzip.open(pathlib.Path(dmp._temp_dir.name) / '32.gz', 'wb') as h:
            h.write(b'1\t\1\t\1\n')

        with mock.patch('pgdumplib.constants.K_OFFSET_POS_SET', 9):
            dmp.save('build/data/dump.test')

        with self.assertRaises(RuntimeError):
            pgdumplib.load('build/data/dump.test')

    def test_table_data_finish_called_with_closed_handle(self):
        with tempfile.TemporaryDirectory() as tempdir:
            table_data = dump.TableData(1024, tempdir, 'UTF8')
            table_data._handle.close()
            table_data.finish()
            table_data.finish()
            self.assertFalse(table_data._handle.closed)

    def test_bad_encoding(self):
        dmp = pgdumplib.new('test')
        dmp.entries[0].defn = 'BAD ENTRY WILL FAIL'
        dmp.save('build/data/dump.test')

        dmp = pgdumplib.load('build/data/dump.test')
        self.assertEqual(dmp.encoding, 'UTF8')

    def test_invalid_desc(self):
        dmp = pgdumplib.new('test')
        with self.assertRaises(ValueError):
            dmp.add_entry('foo', '', 'table')

    def test_invalid_dump_id(self):
        dmp = pgdumplib.new('test')
        with self.assertRaises(ValueError):
            dmp.add_entry(constants.TABLE, '', 'table', dump_id=0)

    def test_used_dump_id(self):
        dmp = pgdumplib.new('test')
        with self.assertRaises(ValueError):
            dmp.add_entry(constants.TABLE, '', 'table', dump_id=1)
