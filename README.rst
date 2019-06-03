pgdumplib
=========

Python3 library for working pg_dump based Postgres backups.

Supports custom, directory, and tar formats.

|Version| |Status| |Coverage| |License|

Installation
------------

.. code::
    pip install pg_dumplib

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


.. |Version| image:: https://img.shields.io/pypi/v/pgdumplib.svg?
   :target: https://pypi.python.org/pypi/pgdumplib

.. |Status| image:: https://circleci.com/gh/gmr/pgdumplib/tree/master.svg?style=svg
   :target: https://circleci.com/gh/gmr/pgdumplib/tree/master

.. |Coverage| image:: https://img.shields.io/codecov/c/github/gmr/pgdumplib.svg?
   :target: https://codecov.io/github/gmr/pgdumplib?branch=master

.. |License| image:: https://img.shields.io/pypi/l/pgdumplib.svg?
   :target: https://pgdumplib.readthedocs.org
