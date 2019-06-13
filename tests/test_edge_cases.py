import gzip
import pathlib
import unittest

import pgdumplib
from pgdumplib import constants


class EdgeTestCase(unittest.TestCase):

    def tearClass(self) -> None:
        test_file = pathlib.Path('build/data/dump.test')
        if test_file.exists():
            test_file.unlink()

    def test_invalid_data_type(self):
        dump = pgdumplib.new('test')
        dump.add_entry('bad', 'block_table', constants.SECTION_DATA, None,
                       'INVALID')
        with gzip.open(
                pathlib.Path(dump._temp_dir.name) / '4.gz', 'wb') as handle:
            handle.write(b'BADDATASHOULDBLOWUPHARD')

        with self.assertRaises(ValueError):
            dump.save('build/data/dump.test')

    def test_invalid_section(self):
        dump = pgdumplib.new('test')
        with self.assertRaises(ValueError):
            dump.add_entry('bad', 'block_table', 'INVALID')

    def test_invalid_dependency(self):
        dump = pgdumplib.new('test')
        with self.assertRaises(ValueError):
            dump.add_entry('bad', 'block_table', dependencies=[1024])
