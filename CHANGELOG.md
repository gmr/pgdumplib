# Changelog

## 3.0.0 - 2019-12-05

- Add support for Postgres 12 / pg_dump archive 1.14.0 format

## 2.1.0 - 2019-11-12

- Add TABLESPACE to supported objects when adding an entry

## 2.0.1 - 2019-11-05

- Minor typing and docstring updates


## 2.0.0 - 2019-10-15

- Add mapping of `desc`/object type to dump section in `pgdumplib.constants`
- Change `pgdumplib.dump.Entry` to use the section mapping instead of as an assignable attribute
- `pgdumplib.dump.Dump.add_entry` function signature change, dropping `section` and moving `desc` to the first arg
- `pgdumplib.dump.Dump.add_entry` validates a provided `dump_id` is above `0` and is not already used
- Change `pgdumplib.dump.Dump.save` behavior to provide a bit of forced ordering and toplogical sorting
- No longer does two-pass writing on save if the dump has no data

## 1.0.1 - 2019-06-17

- Cleanup type annotations
- Distribute as a sdist instead of bdist

## 1.0.0 - 2019-06-14

- Full read/write support of custom formatted pg_dump files
- Refactored API

## 0.2.1 - 2019-01-15

- Documentation and docstring updates

## 0.2.0 - 2019-01-15

-  Make the dump ID value part of the entry

## 0.1.1 - 2019-01-15

- Distribution Fixes

## 0.1.0 - 2018-11-06

- Initial alpha release
