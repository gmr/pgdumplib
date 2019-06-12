"""
pgdumplib exposes a load method to create a :py:class:`~pgdumplib.dump.Dump`
instance from a :command:`pg_dump` file created in the `custom` format.

Once loaded, the table data in a dump is available to read using an iterator:

.. code-block:: python

    import pgdumplib
    from pgdumplib import converters

    dump = pgdumplib.load('path/to/dump', converters.SmartDataConverter)

    for row in dump.read_data('public', 'my_table'):
        print(row)

"""
version = '0.3.0'


def load(filepath: str, converter=None):
    """Load a pg_dump file created with -Fd from disk

    :param str filepath: The path to the dump to load
    :param converter: The data converter class to use
        (Default: :py:class:`pgdumplib.converters.DataConverter`)
    :type converter: :py:class:`pgdumplib.converters.DataConverter`
    :raises: :py:exc:`ValueError`
    :rtype: :py:class:`pgdumplib.dump.Dump`

    """
    from pgdumplib import converters, dump

    return dump.Dump(
        converter=converter or converters.DataConverter).load(filepath)
