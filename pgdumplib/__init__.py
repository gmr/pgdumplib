"""
pgdumplib exposes a load method to create a :py:class:`~pgdumplib.dump.Dump`
instance from a :command:`pg_dump` file created in the `custom` format.

See the :doc:`examples` page to see how to read a dump or create one.

"""
version = '3.1.0'


def load(filepath, converter=None):
    """Load a pg_dump file created with -Fd from disk

    :param os.PathLike filepath: The path to the dump to load
    :param class converter: The data converter class to use
        (Default: :py:class:`pgdumplib.converters.DataConverter`)
    :type converter: pgdumplib.converters.DataConverter or None
    :raises: :py:exc:`ValueError`
    :rtype: pgdumplib.dump.Dump

    """
    from pgdumplib import dump

    return dump.Dump(converter=converter).load(filepath)


def new(dbname: str = 'pgdumplib', encoding: str = 'UTF8',
        converter=None, appear_as: str = '12.0'):
    """Create a new :py:class:`pgdumplib.dump.Dump` instance

    :param dbname: The database name for the dump (Default: ``pgdumplib``)
    :param encoding: The data encoding (Default: ``UTF8``)
    :param converter: The data converter class to use
        (Default: :py:class:`pgdumplib.converters.DataConverter`)
    :type converter: pgdumplib.converters.DataConverter or None
    :param appear_as: The version of Postgres to emulate
        (Default: ``12.0``)
    :rtype: pgdumplib.dump.Dump

    """
    from pgdumplib import dump

    return dump.Dump(dbname, encoding, converter, appear_as)
