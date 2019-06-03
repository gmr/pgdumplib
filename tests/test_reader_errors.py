import tempfile
import unittest
from unittest import mock

from pgdumplib import constants, custom


class TestCase(unittest.TestCase):

    def test_min_version_failure_raises(self):
        min_ver = (constants.MIN_VER[0],
                   constants.MIN_VER[1] + 10,
                   constants.MIN_VER[2])

        with mock.patch('pgdumplib.constants.MIN_VER', min_ver):
            with self.assertRaises(ValueError):
                custom.load('build/data/dump.custom')

    def test_invalid_dump_file(self):
        with tempfile.NamedTemporaryFile('wb') as temp:
            temp.write(b'BADMAGIC')
            with open('build/data/dump.custom', 'rb') as handle:
                temp.write(handle.read())

            with self.assertRaises(ValueError):
                custom.load(temp.name)
