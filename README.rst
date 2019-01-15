pgdumplib
=========
Library for accessing PostgreSQL backups created with pg_dump.

Currently supports directory based dumps and full ToC decoding.

Example Usage
-------------

.. code::

    pg_dump -s -Fd -f foo.dump

.. code::

    import pprint

    from pgdumplib import directory


    reader = directory.Reader('foo.dump')
    print('Header: {}'.format(reader.toc.header))
    print('Database: {}'.format(reader.toc.dbname))
    print('Archive Timestamp: {}'.format(reader.timestamp))
    print('Server Version: {}'.format(reader.server_version))
    print('Dump Version: {}'.format(reader.dump_version))

    for entry in reader.toc.entries:
        pprint.pprint(entry)
