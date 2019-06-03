"""Base common TestCase"""
import dataclasses
import logging
import pathlib
import re
import subprocess
import unittest

import arrow

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

    PATH = 'dump.sql'

    @classmethod
    def setUpClass(cls) -> None:
        cls.local_path = pathlib.Path('build') / 'data' / cls.PATH
        cls.info = cls._read_dump_info(pathlib.Path('/data') / cls.PATH)
        LOGGER.debug('Info: %r', cls.info)

    def setUp(self) -> None:
        self.dump = self._read_dump()
        LOGGER.debug('Dump: %r', self.dump)

    def _read_dump(self):
        raise NotImplementedError

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
                data[key] = arrow.get(match[0]).datetime
            else:
                data[key] = match[0]
        return DumpInfo(**data)

    def test_dump_exists(self):
        self.assertIsNotNone(self.dump)

    def test_toc_compression(self):
        self.assertEqual(self.dump.toc.compression, self.info.compression)

    def test_toc_dbname(self):
        self.assertEqual(self.dump.toc.dbname, 'postgres')

    def test_toc_dump_version(self):
        self.assertEqual(self.dump.toc.dump_version, self.info.pg_dump_version)

    def test_toc_entry_count(self):
        self.assertEqual(len(self.dump.toc.entries), self.info.entry_count)

    def test_toc_server_version(self):
        self.assertEqual(
            self.dump.toc.server_version, self.info.server_version)

    def test_toc_timestamp(self):
        self.assertEqual(self.dump.toc.timestamp, self.info.timestamp)
