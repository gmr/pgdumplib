import logging
import pathlib
import unittest

import pgdumplib

LOGGER = logging.getLogger(__name__)


class TestCase(unittest.TestCase):

    PATH = 'dump.not-compressed'

    def setUp(self):
        self.local_path = pathlib.Path('build') / 'data' / self.PATH
        self.dump = pgdumplib.load(self.local_path)

    def test_redump(self):
        self.dump.path = self.dump.path + '.test'
        self.dump.save()
