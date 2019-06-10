import pathlib
import tempfile
import unittest

from pgdumplib import constants, dump


class TestCase(unittest.TestCase):

    def test_dump_construction(self):
        path = pathlib.Path(tempfile.gettempdir()) / 'test.dump'
        dmp = dump.Dump(path)

        dmp.add_entry(
            tag='ENCODING', desc='ENCODING',
            section=constants.SECTION_PRE_DATA,
            defn="SET client_encoding = 'UTF8';")
        dmp.add_entry(
            tag='STDSTRINGS', desc='STDSTRINGS',
            section=constants.SECTION_PRE_DATA,
            defn="SET standard_conforming_strings = 'on';")
        dmp.add_entry(
            tag='SEARCHPATH', desc='SEARCHPATH',
            section=constants.SECTION_PRE_DATA,
            defn="SELECT pg_catalog.set_config('search_path', '', false);")

        database = dmp.add_entry(
            tag='postgres', desc='DATABASE', owner='postgres',
            section=constants.SECTION_PRE_DATA,
            defn='CREATE DATABASE postgres WITH TEMPLATE = template0 '
                 "ENCODING = 'UTF8' LC_COLLATE = 'en_US.utf8' "
                 "LC_CTYPE = 'en_US.utf8';",
            drop_stmt='DROP DATABASE postgres;')
        dmp.add_entry(
            tag='DATABASE postgres', desc='COMMENT', owner='postgres',
            section=constants.SECTION_PRE_DATA,
            defn="COMMENT ON DATABASE postgres IS 'default administrative "
                 "connection database';",
            dependencies=[database.dump_id])

        dmp.save()
