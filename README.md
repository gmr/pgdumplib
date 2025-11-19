# pgdumplib

Python 3 library for reading and writing pg_dump files using the custom format.

[![Version](https://img.shields.io/pypi/v/pgdumplib.svg)](https://pypi.python.org/pypi/pgdumplib)
[![Status](https://github.com/gmr/pgdumplib/workflows/Testing/badge.svg)](https://github.com/gmr/pgdumplib/actions)
[![Coverage](https://codecov.io/gh/gmr/pgdumplib/branch/master/graph/badge.svg)](https://codecov.io/github/gmr/pgdumplib?branch=master)
[![License](https://img.shields.io/pypi/l/pgdumplib.svg)](https://github.com/gmr/pgdumplib/blob/master/LICENSE)
[![Docs](https://img.shields.io/badge/docs-github%20pages-blue)](https://gmr.github.io/pgdumplib/)

## Installation

```bash
pip install pgdumplib
```

## Example Usage

The following example shows how to create a dump and then read it in, and
iterate through the data of one of the tables.

```bash
pg_dump -d pgbench -Fc -f pgbench.dump
```

```python
import pgdumplib

dump = pgdumplib.load('pgbench.dump')

print('Database: {}'.format(dump.toc.dbname))
print('Archive Timestamp: {}'.format(dump.toc.timestamp))
print('Server Version: {}'.format(dump.toc.server_version))
print('Dump Version: {}'.format(dump.toc.dump_version))

for line in dump.table_data('public', 'pgbench_accounts'):
    print(line)
```
