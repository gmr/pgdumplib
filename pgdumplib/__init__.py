"""
pgdumplib
=========

"""
version = '0.3.0'


def load(filepath):
    """Load a pg_dump file created with -Fd from disk

    :param str filepath: The path to the dump to load
    :raises: :exc:`ValueError`
    :rtype: pgdumplib.reader.Dump

    """
    from pgdumplib import dump

    return dump.Dump().load(filepath)
