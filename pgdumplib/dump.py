"""
Class representing a pg_dump

"""
import decimal
import io
import ipaddress
import logging
import uuid
import zlib

import iso8601

from pgdumplib import constants, exceptions, reader

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
    def __init__(self, path, toc):
        self.path = path
        self.toc = toc

    def __repr__(self):
        return '<Dump path={!r} format={!r} timestamp={!r}>'.format(
            self.path, self.toc.header.format, self.toc.timestamp.isoformat())

    def read_data(self, namespace, table, convert=False):
        """Iterator that returns data for the given namespace and table

        :param str namespace: The namespace/schema for the table
        :param str table: The table name
        :param bool convert: Attempt to convert columns to native data types
        :raises: :exc:`pgdumplib.exceptions.EntityNotFoundError`

        """
        for entry in [e for e in self.toc.entries if e.section == 'Data']:
            if entry.namespace == namespace and entry.tag == table:
                with open(self.path, 'rb') as handle:
                    for line in self._read_entry_data(entry, handle, convert):
                        yield line
                return
        raise exceptions.EntityNotFoundError(namespace=namespace, table=table)

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
            return iso8601.parse_date(column)
        except iso8601.ParseError:
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
