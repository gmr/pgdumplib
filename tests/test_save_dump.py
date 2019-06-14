import dataclasses
import pathlib
import unittest
import uuid

from dateutil import tz
import faker
from faker.providers import date_time

import pgdumplib
from pgdumplib import constants, converters, dump


class SavedDumpTestCase(unittest.TestCase):

    def setUp(self):
        dump = pgdumplib.load('build/data/dump.compressed')
        dump.save('build/data/dump.test')
        self.original = pgdumplib.load('build/data/dump.compressed')
        self.saved = pgdumplib.load('build/data/dump.test')

    def tearDown(self) -> None:
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
        for original in self.original.entries:
            saved_entry = self.saved.get_entry_by_dump_id(original.dump_id)
            for attr in attrs:
                self.assertEqual(
                    getattr(original, attr),
                    getattr(saved_entry, attr),
                    '{} does not match: {} != {}'.format(
                        attr, getattr(original, attr),
                        getattr(saved_entry, attr)))

    def test_table_data_matches(self):
        for entry in range(0, len(self.original.entries)):
            if self.original.entries[entry].desc != constants.TABLE_DATA:
                continue

            original_data = [row for row in self.original.read_table_data(
                self.original.entries[entry].namespace,
                self.original.entries[entry].tag)]

            saved_data = [row for row in self.saved.read_table_data(
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

        example = dump.add_entry(
            'public', 'example', constants.SECTION_PRE_DATA, 'postgres',
            'TABLE',
            'CREATE TABLE public.example (\
              id UUID NOT NULL PRIMARY KEY, \
              created_at TIMESTAMP WITH TIME ZONE, \
              value TEXT NOT NULL);',
            'DROP TABLE public.example')

        columns = 'id', 'created_at', 'value'

        fake = faker.Faker()
        fake.add_provider(date_time)

        rows = [
            (uuid.uuid4(), fake.date_time(tzinfo=tz.tzutc()), 'foo'),
            (uuid.uuid4(), fake.date_time(tzinfo=tz.tzutc()), 'bar'),
            (uuid.uuid4(), fake.date_time(tzinfo=tz.tzutc()), 'baz'),
            (uuid.uuid4(), fake.date_time(tzinfo=tz.tzutc()), 'qux')
        ]

        with dump.table_data_writer(example, columns) as writer:
            for row in rows:
                writer.append(row)

        row = (uuid.uuid4(), fake.date_time(tzinfo=tz.tzutc()), None)
        rows.append(row)

        # Append a second time to get same writer
        with dump.table_data_writer(example, columns) as writer:
            writer.append(row)

        dump.save('build/data/dump.test')

        test_file = pathlib.Path('build/data/dump.test')
        self.assertTrue(test_file.exists())

        dump = pgdumplib.load(test_file, converters.SmartDataConverter)
        entry = dump.get_entry_by_dump_id(1024)
        self.assertEqual(entry.desc, 'DATABASE')
        self.assertEqual(entry.owner, 'postgres')
        self.assertEqual(entry.tag, 'postgres')
        values = [row for row in dump.read_table_data('public', 'example')]
        self.assertListEqual(values, rows)
