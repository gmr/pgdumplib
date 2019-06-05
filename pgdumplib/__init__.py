"""
pgdumplib
=========

"""
import pathlib

from pgdumplib import dump, reader

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
        return dump.Dump(str(path), reader.ToC(handle).read())


def save(filepath, dump):
    """Save a

    :param str filepath: The path to the file to create
    :param pgdumplib.reader.Dump dump: The dump object to write

    """
    pass
