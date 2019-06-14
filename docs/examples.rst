Examples
========

Reading
-------

First, create a dump of your database using :command:`pg_dump` using the ``custom``
format:

.. code-block:: bash

    pg_dump -Fc -d [YOUR] dump.out

The following example shows how to get the data and the table definition
from a dump:

.. code-block:: python

    import pgdumplib

    dump = pgdumplib.load('dump.out')
    for row in dump.read_data('public', 'table-name'):
        print(row)
    print(dump.get_entry('public', 'table-name').defn)

Writing
-------

To create a dump, you need to add sections. The following example shows how to
create a dump with a schema, extension, comment, type, tables, and table data:

.. code-block::
    import datetime
    import uuid

    import pgdumplib
    from pgdumplib import constants

    dump = pgdumpib.new('example')

    schema = dump.add_entry(
        tag='test',
        desc='SCHEMA',
        section=constants.SECTION_PRE_DATA,
        defn='CREATE SCHEMA test;',
        drop_stmt='DROP SCHEMA test;')

    dump.add_entry(
        tag='SCHEMA test',
        desc='ACL',
        section=constants.SECTION_PRE_DATA,
        defn='GRANT USAGE ON SCHEMA test TO PUBLIC;',
        dependencies=[schema.dump_id])

    uuid_ossp = dump.add_entry(
        tag='uuid-ossp',
        desc='EXTENSION',
        section=constants.SECTION_PRE_DATA,
        defn='CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;',
        drop_stmt='DROP EXTENSION "uuid-ossp";')

    dump.add_entry(
        tag='EXTENSION "uuid-ossp"',
        desc='COMMENT',
        section=constants.SECTION_PRE_DATA,
        defn="""\
        COMMENT ON EXTENSION "uuid-ossp"
             IS generate universally unique identifiers (UUIDs)'""",
        dependencies=[uuid_ossp.dump_id])

    addr_type = dump.add_entry(
        namespace='test',
        tag='address_type',
        section=constants.SECTION_PRE_DATA,
        owner='postgres',
        desc='TYPE',
        defn="""\
        CREATE TYPE test.address_type AS ENUM ('billing', delivery');""",
        drop_stmt='DROP TYPE test.address_type;',
        dependencies=[schema.dump_id])

    test_addresses = dump.add_entry(
        namespace='test',
        tag='addresses',
        section=constants.SECTION_PRE_DATA,
        owner='postgres',
        desc='TABLE',
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
        'public', 'example', constants.SECTION_PRE_DATA, 'postgres',
        'TABLE',
        'CREATE TABLE public.example (\
            id UUID NOT NULL PRIMARY KEY,\
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,\
            value TEXT NOT NULL);',
        'DROP TABLE public.example')

    with dump.table_data_writer(example, ['id', 'created_at', 'value']) as writer:
        writer.append(uuid.uuid4(), datetime.datetime.utcnow(), 'row1');
        writer.append(uuid.uuid4(), datetime.datetime.utcnow(), 'row2');
        writer.append(uuid.uuid4(), datetime.datetime.utcnow(), 'row3');
        writer.append(uuid.uuid4(), datetime.datetime.utcnow(), 'row4');
        writer.append(uuid.uuid4(), datetime.datetime.utcnow(), 'row5');

    dump.save('custom.dump')
