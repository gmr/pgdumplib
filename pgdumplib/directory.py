# coding=utf-8
"""
Directory
=========
Implements the Dump class for Directory based

"""
import pathlib

from pgdumplib import reader


class Dump(reader.Dump):
    pass


def load(filepath):
    """Load a pg_dump file created with -Fd

    :raises: `ValueError`
    :rtype: pgdumplib.models.Dump

    """
    path = pathlib.Path(filepath)
    if not path.exists():
        raise ValueError('Path {!r} does not exist'.format(path))

    toc = path / 'toc.dat'
    if not toc.exists():
        raise ValueError('Missing ToC @ {!r}'.format(path))

    with open(toc, 'rb') as handle:
        return Dump(str(path), reader.ToC(handle).read())
