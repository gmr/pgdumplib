import gzip
import pathlib
import struct
import unittest
from unittest import mock

import pgdumplib
from pgdumplib import constants


class EdgeTestCase(unittest.TestCase):

    @staticmethod
    def _write_byte(handle, value) -> None:
        """Write a byte to the handle"""
        handle.write(struct.pack('B', value))

    def _write_int(self, handle, value):
        self._write_byte(handle, 1 if value < 0 else 0)
        if value < 0:
            value = -value
        for offset in range(0, 4):
            self._write_byte(handle, value & 0xFF)
            value >>= 8

    def tearDown(self) -> None:
        test_file = pathlib.Path('build/data/dump.test')
        if test_file.exists():
            test_file.unlink()

    def test_invalid_data_type(self):
        dump = pgdumplib.new('test')
        dump.add_entry(
            'bad', 'entry_desc', constants.SECTION_DATA, None, 'INVALID')
        with gzip.open(
                pathlib.Path(dump._temp_dir.name) / '4.gz', 'wb') as handle:
            handle.write(b'BADDATASHOULDBLOWUPHARD')

        with self.assertRaises(ValueError):
            dump.save('build/data/dump.test')

    def test_invalid_section(self):
        dump = pgdumplib.new('test')
        with self.assertRaises(ValueError):
            dump.add_entry('bad', 'block_table', 'INVALID')

    def test_invalid_dependency(self):
        dump = pgdumplib.new('test')
        with self.assertRaises(ValueError):
            dump.add_entry('bad', 'block_table', dependencies=[1024])

    def test_invalid_block_type_in_data(self):
        dump = pgdumplib.new('test')
        dump.add_entry(
            'bad', 'block_table', constants.SECTION_DATA, None,
            constants.TABLE_DATA, dump_id=1024)
        with gzip.open(
                pathlib.Path(dump._temp_dir.name) / '1024.gz', 'wb') as handle:
            handle.write(b'1\t\1\t\1\n')

        with mock.patch('pgdumplib.constants.BLK_DATA', b'\x02'):
            dump.save('build/data/dump.test')

        with self.assertRaises(RuntimeError):
            pgdumplib.load('build/data/dump.test')

    def test_encoding_not_first_entry(self):
        dump = pgdumplib.new('test', 'LATIN1')
        entries = dump.entries
        dump.entries = [entries[1], entries[2], entries[0]]
        self.assertEqual(dump.encoding, 'LATIN1')
        dump.save('build/data/dump.test')

        dump = pgdumplib.load('build/data/dump.test')
        self.assertEqual(dump.encoding, 'LATIN1')

    def test_encoding_no_entries(self):
        dump = pgdumplib.new('test', 'LATIN1')
        dump.entries = []
        self.assertEqual(dump.encoding, 'LATIN1')
        dump.save('build/data/dump.test')
        dump = pgdumplib.load('build/data/dump.test')
        self.assertEqual(dump.encoding, 'UTF8')

    def test_dump_id_mismatch_in_data(self):
        dump = pgdumplib.new('test')
        dump.add_entry(
            'bad', 'block_table', constants.SECTION_DATA, None,
            constants.TABLE_DATA, dump_id=1024)
        with gzip.open(
                pathlib.Path(dump._temp_dir.name) / '1024.gz', 'wb') as handle:
            handle.write(b'1\t\1\t\1\n')
        dump.save('build/data/dump.test')

        with mock.patch('pgdumplib.dump.Dump._read_block_header') as rbh:
            rbh.return_value = constants.BLK_DATA, 2048
            with self.assertRaises(RuntimeError):
                pgdumplib.load('build/data/dump.test')

    def test_no_data(self):
        dump = pgdumplib.new('test')
        dump.add_entry(
            'bad', 'empty_table', constants.SECTION_DATA, None,
            constants.TABLE_DATA, dump_id=5)
        with gzip.open(pathlib.Path(dump._temp_dir.name) / '5.gz', 'wb') as h:
            h.write(b'')
        dump.save('build/data/dump.test')

        dump = pgdumplib.load('build/data/dump.test')
        data = [line for line in dump.read_data('bad', 'empty_table')]
        self.assertListEqual(data, [])

    def test_runtime_error_when_pos_not_set(self):
        dump = pgdumplib.new('test')
        dump.add_entry('public', 'table', constants.SECTION_DATA, None,
                       constants.TABLE_DATA, dump_id=32)
        with gzip.open(pathlib.Path(dump._temp_dir.name) / '32.gz', 'wb') as h:
            h.write(b'1\t\1\t\1\n')

        dump.save('build/data/dump.test')
        with mock.patch('pgdumplib.constants.K_OFFSET_POS_SET', 9):
            dump.save('build/data/dump.test')

        with self.assertRaises(RuntimeError):
            pgdumplib.load('build/data/dump.test')
