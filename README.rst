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

    import pgdumplib

    dump = pgdumplib.load('/path/to/dump')

    print('Header: {}'.format(dump.toc.header))
    print('Database: {}'.format(dump.toc.dbname))
    print('Archive Timestamp: {}'.format(dump.timestamp))
    print('Server Version: {}'.format(dump.server_version))
    print('Dump Version: {}'.format(dump.dump_version))

    for entry in dump.toc.entries:
        pprint.pprint(entry)
