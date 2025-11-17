import dataclasses
import datetime
import pathlib
import unittest
import uuid

import dotenv
import faker
from faker.providers import date_time

import pgdumplib
from pgdumplib import constants, converters, models

dotenv.load_dotenv()


class SavedDumpTestCase(unittest.TestCase):
    def setUp(self):
        dmp = pgdumplib.load('build/data/dump.compressed')
        dmp.save(pathlib.Path('build/data/dump.test'))
        self.original = pgdumplib.load('build/data/dump.compressed')
        self.saved = pgdumplib.load('build/data/dump.test')

    def tearDown(self) -> None:
        test_file = pathlib.Path('build/data/dump.test')
        if test_file.exists():
            test_file.unlink()

    def test_timestamp_matches(self):
        self.assertEqual(
            self.original.timestamp.isoformat(),
            self.saved.timestamp.isoformat(),
        )

    def test_version_matches(self):
        self.assertEqual(self.original.version, self.saved.version)

    def test_compression_does_not_match(self):
        self.assertTrue(self.original.compression)
        self.assertFalse(self.saved.compression)

    def test_entries_mostly_match(self):
        attrs = [e.name for e in dataclasses.fields(models.Entry)]
        attrs.remove('offset')
        for original in self.original.entries:
            saved_entry = self.saved.get_entry(original.dump_id)
            for attr in attrs:
                self.assertEqual(
                    getattr(original, attr),
                    getattr(saved_entry, attr),
                    f'{attr} does not match: {getattr(original, attr)} != '
                    f'{getattr(saved_entry, attr)}',
                )

    def test_table_data_matches(self):
        for entry in range(0, len(self.original.entries)):
            if self.original.entries[entry].desc != constants.TABLE_DATA:
                continue

            original_data = list(
                self.original.table_data(
                    self.original.entries[entry].namespace,
                    self.original.entries[entry].tag,
                )
            )

            saved_data = list(
                self.saved.table_data(
                    self.original.entries[entry].namespace,
                    self.original.entries[entry].tag,
                )
            )

            for offset in range(0, len(original_data)):
                self.assertListEqual(
                    list(original_data[offset]),
                    list(saved_data[offset]),
                    f'Data in {self.original.entries[entry].namespace}.'
                    f'{self.original.entries[entry].tag} does not match '
                    f'for row {offset}',
                )


class EmptyDumpTestCase(unittest.TestCase):
    def test_empty_dump_has_base_entries(self):
        dmp = pgdumplib.new('test', 'UTF8')
        self.assertEqual(len(dmp.entries), 3)

    def test_empty_save_does_not_err(self):
        dmp = pgdumplib.new('test', 'UTF8')
        dmp.save(pathlib.Path('build/data/dump.test'))
        test_file = pathlib.Path('build/data/dump.test')
        self.assertTrue(test_file.exists())
        test_file.unlink()


class CreateDumpTestCase(unittest.TestCase):
    def tearDown(self) -> None:
        test_file = pathlib.Path('build/data/dump.test')
        if test_file.exists():
            test_file.unlink()

    def test_dump_expectations(self):
        dmp = pgdumplib.new('test', 'UTF8')
        database = dmp.add_entry(
            desc=constants.DATABASE,
            tag='postgres',
            owner='postgres',
            defn="""\
            CREATE DATABASE postgres
              WITH TEMPLATE = template0
                   ENCODING = 'UTF8'
                   LC_COLLATE = 'en_US.utf8'
                   LC_CTYPE = 'en_US.utf8';""",
            drop_stmt='DROP DATABASE postgres',
        )

        dmp.add_entry(
            constants.COMMENT,
            tag='DATABASE postgres',
            owner='postgres',
            defn="""\
            COMMENT ON DATABASE postgres
                 IS 'default administrative connection database';""",
            dependencies=[database.dump_id],
        )

        example = dmp.add_entry(
            constants.TABLE,
            'public',
            'example',
            'postgres',
            'CREATE TABLE public.example (\
              id UUID NOT NULL PRIMARY KEY, \
              created_at TIMESTAMP WITH TIME ZONE, \
              value TEXT NOT NULL);',
            'DROP TABLE public.example',
        )

        columns = 'id', 'created_at', 'value'

        fake = faker.Faker()
        fake.add_provider(date_time)

        rows = [
            (uuid.uuid4(), fake.date_time(tzinfo=datetime.UTC), 'foo'),
            (uuid.uuid4(), fake.date_time(tzinfo=datetime.UTC), 'bar'),
            (uuid.uuid4(), fake.date_time(tzinfo=datetime.UTC), 'baz'),
            (uuid.uuid4(), fake.date_time(tzinfo=datetime.UTC), 'qux'),
        ]

        with dmp.table_data_writer(example, columns) as writer:
            for row in rows:
                writer.append(*row)

        row = (uuid.uuid4(), fake.date_time(tzinfo=datetime.UTC), None)
        rows.append(row)

        # Append a second time to get same writer
        with dmp.table_data_writer(example, columns) as writer:
            writer.append(*row)

        test_file = pathlib.Path('build/data/dump.test')

        dmp.save(test_file)

        self.assertTrue(test_file.exists())

        dmp = pgdumplib.load(test_file, converters.SmartDataConverter)
        entry = dmp.get_entry(database.dump_id)
        self.assertEqual(entry.desc, 'DATABASE')
        self.assertEqual(entry.owner, 'postgres')
        self.assertEqual(entry.tag, 'postgres')
        values = list(dmp.table_data('public', 'example'))
        self.assertListEqual(values, rows)
