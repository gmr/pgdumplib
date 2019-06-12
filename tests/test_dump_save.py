import dataclasses
import unittest

import pgdumplib
from pgdumplib import constants, dump


class SavedDumpTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        dump = pgdumplib.load('build/data/dump.compressed')
        dump.save('build/data/dump.test')
        cls.original = pgdumplib.load('build/data/dump.compressed')
        cls.saved = pgdumplib.load('build/data/dump.test')

    def test_timestamp_matches(self):
        self.assertEqual(self.original.timestamp.isoformat(),
                         self.saved.timestamp.isoformat())

    def test_version_matches(self):
        self.assertEqual(self.original.version, self.saved.version)

    def test_compression_does_not_match(self):
        self.assertTrue(self.original.compression)
        self.assertFalse(self.saved.compression)

    def test_entries_mostly_match(self):
        attrs = [e.name for e in dataclasses.fields(dump.Entry)]
        attrs.remove('offset')
        for offset, original in enumerate(self.original.entries):
            for attr in attrs:
                self.assertEqual(
                    getattr(original, attr),
                    getattr(self.saved.entries[offset], attr),
                    'Offset {} {} does not match: {} != {}'.format(
                        offset, attr, getattr(original, attr),
                        getattr(self.saved.entries[offset], attr)))

    def test_table_data_matches(self):
        for entry in range(0, len(self.original.entries)):
            if self.original.entries[entry].desc != constants.TABLE_DATA:
                continue

            original_data = [row for row in self.original.read_data(
                self.original.entries[entry].namespace,
                self.original.entries[entry].tag)]

            saved_data = [row for row in self.saved.read_data(
                self.original.entries[entry].namespace,
                self.original.entries[entry].tag)]

            for offset in range(0, len(original_data)):
                self.assertListEqual(
                    list(original_data[offset]), list(saved_data[offset]),
                    'Data in {}.{} does not match for row {}'.format(
                        self.original.entries[entry].namespace,
                        self.original.entries[entry].tag,
                        offset))
