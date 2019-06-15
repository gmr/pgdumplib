"""Test Reader"""
import dataclasses
import datetime
import logging
import os
import pathlib
import re
import subprocess
import tempfile
import unittest
from unittest import mock
import uuid

import arrow
from dateutil import tz
import psycopg2

import pgdumplib
from pgdumplib import constants, converters, dump, exceptions

LOGGER = logging.getLogger(__name__)

BLOB_COUNT_SQL = 'SELECT COUNT(*) FROM test.users WHERE icon IS NOT NULL;'

PATTERNS = {
    'timestamp': re.compile(r'\s+Archive created at (.*)\r\n'),
    'dbname': re.compile(r'\s+dbname: (.*)\r\n'),
    'compression': re.compile(r'\s+Compression: (.*)\r\n'),
    'format': re.compile(r'\s+Format: (.*)\r\n'),
    'integer': re.compile(r'\s+Integer: (.*)\r\n'),
    'offset': re.compile(r'\s+Offset: (.*)\r\n'),
    'server_version': re.compile(r'\s+Dumped from database version: (.*)\r\n'),
    'pg_dump_version': re.compile(r'\s+Dumped by [\w_-]+ version: (.*)\r\n'),
    'entry_count': re.compile(r'\s+TOC Entries: (.*)\r\n'),
    'dump_version': re.compile(r'\s+Dump Version: (.*)\r\n')
}


@dataclasses.dataclass
class DumpInfo:
    timestamp: datetime.datetime
    dbname: str
    compression: str
    format: str
    integer: str
    offset: str
    dump_version: str
    server_version: str
    pg_dump_version: str
    entry_count: int


class TestCase(unittest.TestCase):

    PATH = 'dump.not-compressed'
    CONVERTER = converters.DataConverter

    @classmethod
    def setUpClass(cls) -> None:
        cls.dump = pgdumplib.load(
            pathlib.Path('build') / 'data' / cls.PATH, cls.CONVERTER)

    def test_table_data(self):
        data = []
        for line in self.dump.table_data('public', 'pgbench_accounts'):
            data.append(line)
        self.assertEqual(len(data), 100000)

    def test_table_data_empty(self):
        data = []
        for line in self.dump.table_data('test', 'empty_table'):
            data.append(line)
        self.assertEqual(len(data), 0)

    def test_read_dump_entity_not_found(self):
        with self.assertRaises(exceptions.EntityNotFoundError):
            for line in self.dump.table_data('public', 'foo'):
                LOGGER.debug('Line: %r', line)

    def test_lookup_entry(self):
        entry = self.dump.lookup_entry('public', 'pgbench_accounts')
        self.assertEqual(entry.namespace, 'public')
        self.assertEqual(entry.tag, 'pgbench_accounts')
        self.assertEqual(entry.section, constants.SECTION_PRE_DATA)

    def test_lookup_entry_not_found(self):
        self.assertIsNone(self.dump.lookup_entry('public', 'foo'))

    def test_lookup_entry_invalid_section(self):
        with self.assertRaises(ValueError):
            self.dump.lookup_entry('public', 'pgbench_accounts', 'BAD SECTION')

    def test_get_entry(self):
        entry = self.dump.lookup_entry('public', 'pgbench_accounts')
        self.assertEqual(self.dump.get_entry(entry.dump_id), entry)

    def test_get_entry_not_found(self):
        dump_id = max(entry.dump_id for entry in self.dump.entries) + 100
        self.assertIsNone(self.dump.get_entry(dump_id))

    def test_read_blobs(self):
        conn = psycopg2.connect(os.environ['POSTGRES_URI'])
        cursor = conn.cursor()
        cursor.execute(BLOB_COUNT_SQL)
        expectation = cursor.fetchone()[0]
        conn.close()

        dmp = pgdumplib.load(
            pathlib.Path('build') / 'data' / self.PATH, self.CONVERTER)
        blobs = []
        for oid, blob in dmp.blobs():
            self.assertIsInstance(oid, int)
            self.assertIsInstance(blob, bytes)
            blobs.append((oid, blob))
        self.assertEqual(len(blobs), expectation)


class CompressedTestCase(TestCase):

    PATH = 'dump.compressed'


class InsertsTestCase(TestCase):

    CONVERTER = converters.NoOpConverter
    PATH = 'dump.inserts'

    def test_read_dump_data(self):
        count = 0
        for line in self.dump.table_data('public', 'pgbench_accounts'):
            self.assertTrue(
                line.startswith('INSERT INTO public.pgbench_accounts'),
                'Unexpected start @ row {}: {!r}'.format(count, line))
            count += 1
        self.assertEqual(count, 100000)


class NoDataTestCase(TestCase):

    HAS_DATA = False
    PATH = 'dump.no-data'

    def test_table_data(self):
        with self.assertRaises(exceptions.EntityNotFoundError):
            super().test_table_data()

    def test_table_data_empty(self):
        with self.assertRaises(exceptions.EntityNotFoundError):
            super().test_table_data_empty()

    def test_read_blobs(self):
        self.assertEqual(len([b for b in self.dump.blobs()]), 0)


class ErrorsTestCase(unittest.TestCase):

    def test_missing_file_raises_value_error(self):
        path = pathlib.Path(tempfile.gettempdir()) / str(uuid.uuid4())
        with self.assertRaises(ValueError):
            pgdumplib.load(path)

    def test_min_version_failure_raises(self):
        min_ver = (constants.MIN_VER[0],
                   constants.MIN_VER[1] + 10,
                   constants.MIN_VER[2])
        LOGGER.debug('Setting pgdumplib.constants.MIN_VER to %s', min_ver)
        with mock.patch('pgdumplib.constants.MIN_VER', min_ver):
            with self.assertRaises(ValueError):
                pgdumplib.load('build/data/dump.not-compressed')

    def test_max_version_failure_raises(self):
        max_ver = (0, constants.MAX_VER[1], constants.MAX_VER[2])
        LOGGER.debug('Setting pgdumplib.constants.MAX_VER to %s', max_ver)
        with mock.patch('pgdumplib.constants.MAX_VER', max_ver):
            with self.assertRaises(ValueError):
                pgdumplib.load('build/data/dump.not-compressed')

    def test_invalid_dump_file(self):
        with tempfile.NamedTemporaryFile('wb') as temp:
            temp.write(b'PGBAD')
            with open('build/data/dump.not-compressed', 'rb') as handle:
                handle.read(5)
                temp.write(handle.read())

            with self.assertRaises(ValueError):
                pgdumplib.load(temp.name)


class NewDumpTestCase(unittest.TestCase):

    def test_pgdumplib_new(self):
        dmp = pgdumplib.new('test', 'UTF8', converters.SmartDataConverter)
        self.assertIsInstance(dmp, dump.Dump)
        self.assertIsInstance(dmp._converter, converters.SmartDataConverter)


class RestoreComparisonTestCase(unittest.TestCase):

    PATH = 'dump.not-compressed'

    @classmethod
    def setUpClass(cls) -> None:
        cls.local_path = pathlib.Path('build') / 'data' / cls.PATH
        cls.info = cls._read_dump_info(pathlib.Path('/data') / cls.PATH)
        LOGGER.debug('Info: %r', cls.info)

    def setUp(self) -> None:
        self.dump = self._read_dump()
        LOGGER.debug('Dump: %r', self.dump)

    def _read_dump(self):
        return pgdumplib.load(self.local_path)

    @classmethod
    def _read_dump_info(cls, remote_path) -> DumpInfo:
        restore = subprocess.run(
            ['docker-compose', 'exec', 'postgres',
             'pg_restore', '-l', str(remote_path)],
            capture_output=True)
        assert restore.returncode == 0
        stdout = restore.stdout.decode('utf-8')
        data = {}
        for key, pattern in PATTERNS.items():
            match = pattern.findall(stdout)
            if not match:
                LOGGER.warning('No match for %s', key)
            elif key == 'compression':
                data[key] = match[0] != '0'
            elif key == 'entry_count':
                data[key] = int(match[0])
            elif key == 'timestamp':
                data[key] = arrow.get(
                    match[0]).replace(tzinfo=tz.tzlocal()).datetime
            else:
                data[key] = match[0]
        return DumpInfo(**data)

    def test_dump_exists(self):
        self.assertIsNotNone(self.dump)

    def test_toc_compression(self):
        self.assertEqual(self.dump.compression, self.info.compression)

    def test_toc_dbname(self):
        self.assertEqual(self.dump.dbname, 'postgres')

    def test_toc_dump_version(self):
        self.assertEqual(self.dump.dump_version, self.info.pg_dump_version)

    def test_toc_entry_count(self):
        self.assertEqual(len(self.dump.entries), self.info.entry_count)

    def test_toc_server_version(self):
        self.assertEqual(
            self.dump.server_version, self.info.server_version)

    def test_toc_timestamp(self):
        self.assertEqual(
            self.dump.timestamp.isoformat(), self.info.timestamp.isoformat())


class RestoreComparisonCompressedTestCase(RestoreComparisonTestCase):

    PATH = 'dump.compressed'


class RestoreComparisonNoDataTestCase(RestoreComparisonTestCase):

    HAS_DATA = False
    PATH = 'dump.no-data'


class RestoreComparisonDataOnlyTestCase(RestoreComparisonTestCase):

    PATH = 'dump.data-only'
