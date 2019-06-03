"""Test Tar Archives"""
import pathlib
import tempfile
import uuid

from pgdumplib import directory
from . import base


class TestCase(base.TestCase):

    PATH = 'dump.directory'
    FORMAT = 'Directory'

    def _read_dump(self):
        return directory.load(self.local_path)

    def test_missing_file_raises_value_error(self):
        path = pathlib.Path(tempfile.gettempdir()) / str(uuid.uuid4())
        with self.assertRaises(ValueError):
            directory.load(path)

    def test_missing_toc_raises_value_error(self):
        with tempfile.TemporaryDirectory() as path:
            with self.assertRaises(ValueError):
                directory.load(path)
