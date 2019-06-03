import pathlib
import tempfile
import unittest
import uuid

import pgdumplib
from pgdumplib import custom, directory, tar


class TestCase(unittest.TestCase):

    def test_custom_matches(self):
        archive_path = pathlib.Path('build/data/dump.custom')
        value1 = pgdumplib.load(archive_path)
        value2 = custom.load(archive_path)
        self.assertEqual(value1.toc, value2.toc)

    def test_directory_matches(self):
        archive_path = pathlib.Path('build/data/dump.directory')
        value1 = pgdumplib.load(archive_path)
        value2 = directory.load(archive_path)
        self.assertEqual(value1.toc, value2.toc)

    def test_tar_matches(self):
        archive_path = pathlib.Path('build/data/dump.tar')
        value1 = pgdumplib.load(archive_path)
        value2 = tar.load(archive_path)
        self.assertEqual(value1.toc, value2.toc)

    def test_missing_file_raises_value_error(self):
        path = pathlib.Path(tempfile.gettempdir()) / str(uuid.uuid4())
        with self.assertRaises(ValueError):
            pgdumplib.load(path)
