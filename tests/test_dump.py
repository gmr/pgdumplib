"""Test Reader"""
import dataclasses
import logging
import pathlib
import re
import subprocess
import tempfile
import unittest
from unittest import mock
import uuid

import arrow
from dateutil import tz

import pgdumplib
from pgdumplib import constants, exceptions

LOGGER = logging.getLogger(__name__)

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
    timestamp: str
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

    def test_read_dump_data(self):
        data = []
        for line in self.dump.read_data('public', 'pgbench_accounts'):
            data.append(line)
        self.assertEqual(len(data), 100000)

    def test_read_dump_entity_not_found(self):
        with self.assertRaises(exceptions.EntityNotFoundError):
            for line in self.dump.read_data('public', str(uuid.uuid4())):
                LOGGER.debug('Line: %r', line)


class CompressedTestCase(TestCase):

    PATH = 'dump.compressed'


class NoDataTestCase(TestCase):

    HAS_DATA = False
    PATH = 'dump.no-data'

    def test_read_dump_data(self):
        with self.assertRaises(exceptions.EntityNotFoundError):
            for line in self.dump.read_data('public', 'pgbench_accounts'):
                LOGGER.debug('Line: %r', line)


class DataOnlyTestCase(TestCase):

    PATH = 'dump.data-only'


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
