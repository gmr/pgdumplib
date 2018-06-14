# coding=utf-8
"""
Directory Reader
================

"""
from pgdumplib import toc


class Reader:
    """Reads dumps created with the -Fd option."""

    def __init__(self, directory):
        self.directory = directory
        self.toc = toc.ToC('{}/toc.dat'.format(directory))

    @property
    def dbname(self):
        return self.toc.dbname

    @property
    def dump_version(self):
        return self.toc.dump_version

    @property
    def server_version(self):
        return self.toc.server_version

    @property
    def timestamp(self):
        return self.toc.timestamp
