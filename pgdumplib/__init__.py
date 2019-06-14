"""
pgdumplib exposes a load method to create a :py:class:`~pgdumplib.dump.Dump`
instance from a :command:`pg_dump` file created in the `custom` format.

See the :doc:`examples` page to see how to read a dump or create one.

"""
version = '1.0.0'


def load(filepath: str, converter=None):
    """Load a pg_dump file created with -Fd from disk

    :param str filepath: The path to the dump to load
    :param class converter: The data converter class to use
        (Default: :py:class:`pgdumplib.converters.DataConverter`)
    :raises: :py:exc:`ValueError`
    :rtype: :py:class:`pgdumplib.dump.Dump`

    """
    from pgdumplib import dump

    return dump.Dump(converter=converter).load(filepath)


def new(dbname: str = 'pgdumplib', encoding: str = 'UTF8',
        converter=None):
    """Create a new :py:class:`pgdumplib.dump.Dump` instance

    :param str dbname: The database name for the dump (Default: ``pgdumplib``)
    :param str encoding: The data encoding (Default: ``UTF8``)
    :param converter: The data converter class to use
        (Default: :py:class:`pgdumplib.converters.DataConverter`)
    :rtype: :py:class:`pgdumplib.dump.Dump`

    """
    from pgdumplib import dump

    return dump.Dump(dbname, encoding, converter)
