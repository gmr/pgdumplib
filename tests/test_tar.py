"""Test Tar Archives"""
import pathlib
import tempfile
import uuid

from pgdumplib import tar
from . import base


class TestCase(base.TestCase):

    PATH = 'dump.tar'
    FORMAT = 'TAR'

    def _read_dump(self):
        return tar.load(self.local_path)

    def test_missing_file_raises_value_error(self):
        path = pathlib.Path(tempfile.gettempdir()) / str(uuid.uuid4())
        with self.assertRaises(ValueError):
            tar.load(path)

    def test_bad_file_raises_value_error(self):
        path = str(self.local_path).replace('.tar', '.custom')
        with self.assertRaises(ValueError):
            tar.load(path)
