import dataclasses
import pathlib
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

    @classmethod
    def tearDownClass(cls) -> None:
        test_file = pathlib.Path('build/data/dump.test')
        if test_file.exists():
            test_file.unlink()

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


class EmptyDumpTestCase(unittest.TestCase):

    def test_empty_dump_has_base_entries(self):
        dump = pgdumplib.new('test', 'UTF8')
        self.assertEqual(len(dump.entries), 3)

    def test_empty_save_does_not_err(self):
        dump = pgdumplib.new('test', 'UTF8')
        dump.save('build/data/dump.test')
        test_file = pathlib.Path('build/data/dump.test')
        self.assertTrue(test_file.exists())
        test_file.unlink()


class CreateDumpTestCase(unittest.TestCase):

    def tearClass(self) -> None:
        test_file = pathlib.Path('build/data/dump.test')
        if test_file.exists():
            test_file.unlink()

    def test_dump_exepctations(self):
        dump = pgdumplib.new('test', 'UTF8')
        entry = dump.add_entry(
            None, 'postgres', constants.SECTION_PRE_DATA, 'postgres',
            'DATABASE',
            """\
            CREATE DATABASE postgres
              WITH TEMPLATE = template0
                   ENCODING = 'UTF8'
                   LC_COLLATE = 'en_US.utf8'
                   LC_CTYPE = 'en_US.utf8';""",
            'DROP DATABASE postgres',
            dump_id=1024)

        dump.add_entry(
            None, 'DATABASE postgres', constants.SECTION_PRE_DATA,
            'postgres', 'COMMENT',
            """\
            COMMENT ON DATABASE postgres
                 IS 'default administrative connection database';""",
            dependencies=[entry.dump_id])

        dump.save('build/data/dump.test')

        test_file = pathlib.Path('build/data/dump.test')
        self.assertTrue(test_file.exists())

        dump = dump.load(test_file)
        entry = dump.get_entry_by_dump_id(1024)
        self.assertEqual(entry.desc, 'DATABASE')
        self.assertEqual(entry.owner, 'postgres')
        self.assertEqual(entry.tag, 'postgres')
