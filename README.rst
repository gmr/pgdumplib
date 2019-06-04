pgdumplib
=========

Python3 library for working with Postgres

|Version| |Status| |Coverage| |License|

Installation
------------

.. code::
    pip install pg_dumplib

Example Usage
-------------

.. code::

    pg_dump -s -Fd -f pgbench.dump

.. code::

    import pgdumplib

    dump = pgdumplib.load('pgbench.dump')

    print('Header: {}'.format(dump.toc.header))
    print('Database: {}'.format(dump.toc.dbname))
    print('Archive Timestamp: {}'.format(dump.toc.timestamp))
    print('Server Version: {}'.format(dump.toc.server_version))
    print('Dump Version: {}'.format(dump.toc.dump_version))

    for line in dump.read_data('public', 'pgbench_accounts'):
        print(line)

.. |Version| image:: https://img.shields.io/pypi/v/pgdumplib.svg?
   :target: https://pypi.python.org/pypi/pgdumplib

.. |Status| image:: https://img.shields.io/circleci/build/gh/gmr/pgdumplib/master.svg?token=46593b052a2e0ff4720cfa2fc52bd6ef738ec989
   :target: https://circleci.com/gh/gmr/pgdumplib/tree/master

.. |Coverage| image:: https://codecov.io/gh/gmr/pgdumplib/branch/master/graph/badge.svg
   :target: https://codecov.io/github/gmr/pgdumplib?branch=master

.. |License| image:: https://img.shields.io/pypi/l/pgdumplib.svg?
   :target: https://pgdumplib.readthedocs.org



