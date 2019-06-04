"""
pgdumplib
=========

"""
import pathlib

from pgdumplib import reader

version = '0.3.0'


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
        return reader.Dump(str(path), reader.ToC(handle).read())
