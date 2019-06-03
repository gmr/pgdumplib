"""
pgdumplib
=========

"""
import pathlib

from pgdumplib import custom, directory, tar

version = '0.3.0'


def load(filepath):
    """Load a pg_dump file from disk, supporting custom, directory, or tar
    format.

    :param str filepath: The path to the dump to load
    :raises: :exc:`ValueError`
    :rtype: pgdumplib.modules.Dump

    """
    path = pathlib.Path(filepath)
    if path.is_dir():
        return directory.load(path)
    elif path.name.endswith('.tar'):
        return tar.load(path)
    else:
        return custom.load(path)
