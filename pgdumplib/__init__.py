"""
pgdumplib
=========

"""
import pathlib

from pgdumplib import dump, reader
from pgdumplib.__version__ import version


def load(filepath):
    """Load a pg_dump file created with -Fd from disk

    :param str filepath: The path to the dump to load
    :raises: :exc:`ValueError`
    :rtype: pgdumplib.reader.Dump

    """
    path = pathlib.Path(filepath)
    if not path.exists():
        raise ValueError('Path {!r} does not exist'.format(path))

    with open(path, 'rb') as handle:
        return dump.Dump(str(path), reader.ToC(handle).read())


__all__ = [
    'load',
    'version'
]
