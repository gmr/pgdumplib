import dataclasses
import datetime
import os
import pathlib
import subprocess
import tempfile
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
        self.assertNotEqual(
            self.original.compression_algorithm, constants.COMPRESSION_NONE
        )
        self.assertEqual(
            self.saved.compression_algorithm, constants.COMPRESSION_NONE
        )

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

            # Type narrowing for namespace and tag
            namespace = self.original.entries[entry].namespace
            tag = self.original.entries[entry].tag
            assert namespace is not None  # noqa: S101
            assert tag is not None  # noqa: S101

            original_data = list(self.original.table_data(namespace, tag))

            saved_data = list(self.saved.table_data(namespace, tag))

            for offset in range(0, len(original_data)):
                self.assertListEqual(
                    list(original_data[offset]),
                    list(saved_data[offset]),
                    f'Data in {namespace}.{tag} does not match '
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
        self.assertIsNotNone(entry)
        assert entry is not None  # noqa: S101 - Type narrowing for type checker
        self.assertEqual(entry.desc, 'DATABASE')
        self.assertEqual(entry.owner, 'postgres')
        self.assertEqual(entry.tag, 'postgres')
        values = list(dmp.table_data('public', 'example'))
        self.assertListEqual(values, rows)


class Issue6RegressionTestCase(unittest.TestCase):
    """Regression test for GitHub issue #6: empty tableam causing invalid SQL

    When pgdumplib loads and saves a dump, it should not generate entries
    with empty tableam values that cause pg_restore to generate invalid SQL
    like: SET default_table_access_method = "";
    """

    def test_load_save_no_empty_tableam_sql(self):
        """Test that resaved dumps don't generate invalid empty tableam SQL"""
        # Create a test dump with a table (has tableam) and others without
        dmp = pgdumplib.new('test_issue6', 'UTF8', appear_as='13.1')

        # Add a table with tableam='heap'
        dmp.add_entry(
            constants.TABLE,
            namespace='public',
            tag='test_table',
            owner='postgres',
            defn='CREATE TABLE public.test_table (id integer);',
            tableam='heap',
        )

        # Add a sequence (should not have tableam set)
        dmp.add_entry(
            constants.SEQUENCE,
            namespace='public',
            tag='test_seq',
            owner='postgres',
            defn='CREATE SEQUENCE public.test_seq;',
        )

        # Save and reload
        with tempfile.NamedTemporaryFile(delete=False, suffix='.dump') as tmp:
            tmp_path = tmp.name

        try:
            dmp.save(tmp_path)

            # Check that pg_restore can generate valid SQL
            result = subprocess.run(  # noqa: S603
                ['pg_restore', '--schema-only', '-f', '-', tmp_path],  # noqa: S607
                capture_output=True,
                text=True,
            )

            # Should not fail
            self.assertEqual(
                result.returncode,
                0,
                f'pg_restore failed: {result.stderr}',
            )

            # Should not contain invalid empty SET statement
            self.assertNotIn(
                'SET default_table_access_method = ""',
                result.stdout,
                'Found invalid empty tableam SET statement in output',
            )

            # Should contain the valid SET statement for the table
            self.assertIn(
                'SET default_table_access_method = heap',
                result.stdout,
                'Missing valid tableam SET statement for table',
            )
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
