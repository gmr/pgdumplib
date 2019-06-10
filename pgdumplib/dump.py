"""
Class representing a pg_dump

"""
import decimal
import io
import ipaddress
import logging
import uuid
import zlib

import arrow
from arrow import parser

from pgdumplib import constants, exceptions, models, reader, writer

LOGGER = logging.getLogger(__name__)


def _skip_data(handle, intsize):
    """Skip data from current file position.
    Data blocks are formatted as an integer length, followed by data.
    A zero length denoted the end of the block.

    :param file object handle: The file handle to skip data from
    :param int intsize: The size of the integers in the dump file
    :return:

    """
    block_length, buff_len = reader.read_int(handle, intsize), 0
    while block_length:
        if block_length > buff_len:
            buff_len = block_length
        data_in = handle.read(block_length)
        if len(data_in) != block_length:
            LOGGER.error('Failure to read full block (%i != %i)',
                         len(data_in), block_length)
            raise ValueError()
        block_length = reader.read_int(handle, intsize)


class Dump:
    """Dump Object containing data about the dump and includes methods for
    reading data out of the dump.

    """
    def __init__(self, path, toc=None):
        self.path = path
        self.toc = toc or models.ToC()

    def __repr__(self):
        return '<Dump path={!r} format={!r} timestamp={!r}>'.format(
            self.path, self.toc.header.format, self.toc.timestamp.isoformat())

    def add_entry(self, namespace=None, tag=None,
                  section=constants.SECTION_NONE, owner=None, desc=None,
                  tablespace=None, defn=None, drop_stmt=None, copy_stmt=None,
                  dependencies=None, dump_id=None):
        """Add an entry to the dump

        The `namespace`, `tag`, and `section` are required.

        A :exc:`ValueError` will be raised if section is not one of
        :const:`~pgdumplib.constants.SECTION_NONE`,
        :const:`~pgdumplib.constants.SECTION_PRE_DATA`,
        :const:`~pgdumplib.constants.SECTION_DATA`, or
        :const:`~pgdumplib.constants.SECTION_POST_DATA`.

        When adding data, is is advised to invoke :meth:`~Dump.add_data`
        instead of invoking :meth:`~Dump.add_entry` directly.

        If `dependencies` are specified, they will be validated and if a
        `dump_id` is specified and no entry is found with that `dump_id`,
        a :exc:`ValueError` will be raised.

        Other omitted values will be set to the default values will be set to
        the defaults specified in the :class:`~pgdumplib.models.Entry` class.

        The `dump_id` will be auto-calculated based upon the existing entries
        if it is not specified.

        :param str namespace: The namespace of the entry
        :param str tag: The name/table/relation/etc of the entry
        :param str section: The section for the entry
        :param str owner: The owner of the object in Postgres
        :param str desc: The entry description
        :param str tablespace: The tablespace to use
        :param str defn: The DDL definition for the entry
        :param drop_stmt: A drop statement used to drop the entry before
        :param copy_stmt: A copy statement used when there is a corresponding
            data section.
        :param list dependencies: A list of dump_ids of objects that the entry
            is dependent upon.
        :param int dump_id: The dump id, will be auto-calculated if left empty
        :raises: ValueError
        :rtype: pgdumplib.models.Entry

        """
        if section not in constants.SECTIONS:
            raise ValueError('Invalid section: {}'.format(section))

        dump_ids = [e.dump_id for e in self.toc.entries]

        for dependency in dependencies or []:
            if dependency not in dump_ids:
                raise ValueError(
                    'Dependency dump_id {!r} not found'.format(dependency))

        if not dump_id:
            dump_id = max(dump_ids) + 1 if dump_ids else 1

        self.toc.entries.append(models.Entry(
            dump_id, False, None, None, tag, desc, section, defn, drop_stmt,
            copy_stmt, namespace, tablespace, owner, False, dependencies))
        return self.toc.entries[-1]

    def get_entry(self, namespace, tag, section=constants.SECTION_PRE_DATA):
        """Return the entry for the given namespace and tag

        :param str namespace: The namespace of the entry
        :param str tag: The tag/relation/table name
        :param str section: The dump section the entry is for
        :raises: ValueError
        :rtype: pgdumplib.models.Entry or None

        """
        if section not in constants.SECTIONS:
            raise ValueError('Invalid section: {}'.format(section))
        for entry in [e for e in self.toc.entries if e.section == section]:
            if entry.namespace == namespace and entry.tag == tag:
                return entry

    def get_entry_by_dump_id(self, dump_id):
        """Return the entry for the given `dump_id`

        :param int dump_id: The dump ID of the entry to return.
        :rtype: pgdumplib.models.Entry or None

        """
        for entry in self.toc.entries:
            if entry.dump_id == dump_id:
                return entry

    def read_data(self, namespace, table, convert=False):
        """Iterator that returns data for the given namespace and table

        :param str namespace: The namespace/schema for the table
        :param str table: The table name
        :param bool convert: Attempt to convert columns to native data types
        :raises: :exc:`pgdumplib.exceptions.EntityNotFoundError`

        """
        for entry in [e for e in self.toc.entries
                      if e.section == constants.SECTION_DATA]:
            if entry.namespace == namespace and entry.tag == table:
                with open(self.path, 'rb') as handle:
                    for line in self._read_entry_data(entry, handle, convert):
                        yield line
                return
        raise exceptions.EntityNotFoundError(namespace=namespace, table=table)

    def save(self):
        """Save the Dump file to `Dump.path`"""
        with open(self.path, 'wb') as handle:
            writer.Writer(handle).write(self)

    def _read_block_header(self, handle):
        """Read the block header in

        :param file object handle: The file handle to read data from
        :rtype: bytes, int

        """
        return handle.read(1), reader.read_int(handle, self.toc.header.intsize)

    def _read_data_compressed(self, handle):
        """Read a compressed data block

        :param file object handle: The file handle to read data from
        :rtype: str

        """
        buffer = io.BytesIO()
        chunk = b''
        decompress = zlib.decompressobj()
        block_length = constants.ZLIB_IN_SIZE
        while block_length == constants.ZLIB_IN_SIZE:
            block_length = reader.read_int(handle, self.toc.header.intsize)
            chunk += handle.read(constants.ZLIB_IN_SIZE)
            buffer.write(decompress.decompress(chunk))
            chunk = decompress.unconsumed_tail
        return buffer.getvalue().decode('utf-8')

    def _read_data_uncompressed(self, handle):
        """Read an uncompressed data block

        :param file object handle: The file handle to read data from

        """
        buffer = io.BytesIO()
        while True:
            block_length = reader.read_int(handle, self.toc.header.intsize)
            if not block_length:
                break
            buffer.write(handle.read(block_length))
        return buffer.getvalue().decode('utf-8')

    def _read_entry_data(self, entry, handle, convert):
        """Read the data from the entry

        :param pgdumplib.models.Entry entry: The entry to read
        :param file object handle: The file handle to read data from
        :param bool convert: Attempt to convert the data to native types

        """
        if entry.data_state == constants.K_OFFSET_NO_DATA:
            return
        elif entry.data_state == constants.K_OFFSET_POS_NOT_SET:
            block_type, dump_id = self._read_block_header(handle)
            while block_type != constants.EOF and dump_id != entry.dump_id:
                if block_type in [constants.BLK_DATA, constants.BLK_BLOBS]:
                    _skip_data(handle, self.toc.header.intsize)
                else:
                    LOGGER.warning('Unknown block type: %r', block_type)
                block_type, dump_id = self._read_block_header(handle)
        else:
            handle.seek(entry.offset, io.SEEK_SET)

        block_type, dump_id = self._read_block_header(handle)
        if dump_id != entry.dump_id:
            raise ValueError('Dump IDs do not match')

        if block_type == constants.BLK_DATA:
            if self.toc.compression:
                buffer = self._read_data_compressed(handle)
            else:
                buffer = self. _read_data_uncompressed(handle)
            for line in buffer.split('\n'):
                if line.startswith('\\.'):
                    break
                yield self._convert_row(line) if convert else line.split('\t')
        else:
            raise ValueError(
                'Unsupported block type: {}'.format(block_type))

    @staticmethod
    def _convert_column(column):
        """Attempt to convert the column from a string if appropriate

        :param str column: The column to attempt to convert
        :rtype: mixed

        """
        if column == '\\N':
            return None
        elif column.isnumeric():
            try:
                return int(column)
            except ValueError:
                pass
        elif column.isdecimal():
            try:
                return decimal.Decimal(column)
            except ValueError:
                pass
        try:
            return ipaddress.ip_address(column)
        except ValueError:
            pass
        try:
            return ipaddress.ip_network(column)
        except ValueError:
            pass
        try:
            return arrow.get(column).datetime
        except parser.ParserError:
            pass
        try:
            return uuid.UUID(column)
        except ValueError:
            pass
        return column

    def _convert_row(self, row):
        """Split the fields and convert the columns to Python naturalized
        data types.

        :param str row: The row to convert
        :rtype: list

        """
        return [self._convert_column(col) for col in row.split('\t')]
