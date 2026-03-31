import logging
import pathlib
import unittest

import pgdumplib
from pgdumplib import constants

LOGGER = logging.getLogger(__name__)


class EdgeTestCase(unittest.TestCase):
    def tearDown(self) -> None:
        test_file = pathlib.Path('build/data/dump.test')
        if test_file.exists():
            test_file.unlink()

    def test_invalid_dependency(self):
        dmp = pgdumplib.new('test')
        with self.assertRaises(ValueError):
            dmp.add_entry(
                constants.TABLE, '', 'block_table', dependencies=[1024]
            )

    def test_bad_encoding(self):
        dmp = pgdumplib.new('test')
        # Modify the encoding entry via update_entry

        dmp._dump.update_entry(1, defn='BAD ENTRY WILL FAIL')
        dmp.save('build/data/dump.test')

        dmp = pgdumplib.load('build/data/dump.test')
        self.assertEqual(dmp.encoding, 'UTF8')

    def test_invalid_desc(self):
        dmp = pgdumplib.new('test')
        with self.assertRaises(ValueError):
            dmp.add_entry('foo', '', 'table')

    def test_invalid_dump_id(self):
        dmp = pgdumplib.new('test')
        with self.assertRaises(ValueError):
            dmp.add_entry(constants.TABLE, '', 'table', dump_id=0)

    def test_used_dump_id(self):
        dmp = pgdumplib.new('test')
        with self.assertRaises(NotImplementedError):
            dmp.add_entry(constants.TABLE, '', 'table', dump_id=1)
