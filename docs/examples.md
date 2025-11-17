# Examples

## Reading

First, create a dump of your database using `pg_dump` using the `custom` format:

```bash
pg_dump -Fc -d [YOUR] dump.out
```

The following example shows how to get the data and the table definition from a dump:

```python
import pgdumplib
from pgdumplib import constants

dump = pgdumplib.load('dump.out')
for row in dump.table_data('public', 'table-name'):
    print(row)
print(dump.lookup_entry(constants.TABLE, 'public', 'table-name').defn)
```

## Writing

To create a dump, you need to add sections. The following example shows how to create a dump with a schema, extension, comment, type, tables, and table data:

```python
import datetime
import uuid

import pgdumplib
from pgdumplib import constants

dump = pgdumplib.new('example')

schema = dump.add_entry(
    desc=constants.SCHEMA,
    tag='test',
    defn='CREATE SCHEMA test;',
    drop_stmt='DROP SCHEMA test;')

dump.add_entry(
    desc=constants.ACL,
    tag='SCHEMA test',
    defn='GRANT USAGE ON SCHEMA test TO PUBLIC;',
    dependencies=[schema.dump_id])

uuid_ossp = dump.add_entry(
    desc=constants.EXTENSION,
    tag='uuid-ossp',
    defn='CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;',
    drop_stmt='DROP EXTENSION "uuid-ossp";')

dump.add_entry(
    desc=constants.COMMENT,
    tag='EXTENSION "uuid-ossp"',
    defn="""COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)'""",
    dependencies=[uuid_ossp.dump_id])

addr_type = dump.add_entry(
    desc=constants.TYPE,
    namespace='test',
    tag='address_type',
    owner='postgres',
    defn="""\
    CREATE TYPE test.address_type AS ENUM ('billing', 'delivery');""",
    drop_stmt='DROP TYPE test.address_type;',
    dependencies=[schema.dump_id])

test_addresses = dump.add_entry(
    desc=constants.TABLE,
    namespace='test',
    tag='addresses',
    owner='postgres',
    defn="""\
    CREATE TABLE addresses (
        id               UUID                     NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
        created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_modified_at TIMESTAMP WITH TIME ZONE,
        user_id          UUID                     NOT NULL REFERENCES users (id) ON DELETE CASCADE ON UPDATE CASCADE,
        type             address_type             NOT NULL,
        address1         TEXT                     NOT NULL,
        address2         TEXT,
        address3         TEXT,
        locality         TEXT                     NOT NULL,
        region           TEXT,
        postal_code      TEXT                     NOT NULL,
        country          TEXT                     NOT NULL
    );""",
    drop_stmt='DROP TABLE test.addresses;',
    dependencies=[schema.dump_id, addr_type.dump_id, uuid_ossp.dump_id])

example = dump.add_entry(
    constants.TABLE,
    'public', 'example', 'postgres',
    'CREATE TABLE public.example (\
        id UUID NOT NULL PRIMARY KEY,\
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,\
        value TEXT NOT NULL);',
    'DROP TABLE public.example')

with dump.table_data_writer(example, ['id', 'created_at', 'value']) as writer:
    writer.append(uuid.uuid4(), datetime.datetime.utcnow(), 'row1')
    writer.append(uuid.uuid4(), datetime.datetime.utcnow(), 'row2')
    writer.append(uuid.uuid4(), datetime.datetime.utcnow(), 'row3')
    writer.append(uuid.uuid4(), datetime.datetime.utcnow(), 'row4')
    writer.append(uuid.uuid4(), datetime.datetime.utcnow(), 'row5')

dump.save('custom.dump')
```
