"""Test Tar Archives"""
import pathlib
import tempfile
import uuid

from pgdumplib import custom
from . import base


class TestCase(base.TestCase):

    PATH = 'dump.custom'
    FORMAT = 'Custom'

    def _read_dump(self):
        return custom.load(self.local_path)

    def test_missing_file_raises_value_error(self):
        path = pathlib.Path(tempfile.gettempdir()) / str(uuid.uuid4())
        with self.assertRaises(ValueError):
            custom.load(path)
