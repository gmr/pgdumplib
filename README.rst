pgdumplib
=========

Python3 library for reading and writing pg_dump files using the custom format.

|Version| |Status| |Coverage| |License| |Docs|

Installation
------------

.. code-block:: bash

    pip install pgdumplib

Example Usage
-------------

The following example shows how to create a dump and then read it in, and
iterate through the data of one of the tables.

.. code-block:: python

    pg_dump -d pgbench -Fc -f pgbench.dump

.. code-block:: python

    import pgdumplib

    dump = pgdumplib.load('pgbench.dump')

    print('Database: {}'.format(dump.toc.dbname))
    print('Archive Timestamp: {}'.format(dump.toc.timestamp))
    print('Server Version: {}'.format(dump.toc.server_version))
    print('Dump Version: {}'.format(dump.toc.dump_version))

    for line in dump.table_data('public', 'pgbench_accounts'):
        print(line)

.. |Version| image:: https://img.shields.io/pypi/v/pgdumplib.svg
   :target: https://pypi.python.org/pypi/pgdumplib
   :alt: Package Version

.. |Status| image:: https://github.com/gmr/pgdumplib/workflows/Testing/badge.svg
   :target: https://github.com/gmr/pgdumplib/actions
   :alt: Build Status

.. |Coverage| image:: https://codecov.io/gh/gmr/pgdumplib/branch/master/graph/badge.svg
   :target: https://codecov.io/github/gmr/pgdumplib?branch=master
   :alt: Code Coverage

.. |License| image:: https://img.shields.io/pypi/l/pgdumplib.svg
   :target: https://github.com/gmr/pgdumplib/blob/master/LICENSE
   :alt: BSD

.. |Docs| image:: https://img.shields.io/readthedocs/pgdumplib.svg
   :target: https://pgdumplib.readthedocs.io/
   :alt: Documentation Status
