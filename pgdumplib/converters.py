"""
Data Converters

"""
import decimal
import ipaddress
import uuid

import arrow
from arrow import parser


class DataConverter:
    """Base Row/Column Converter

    Base class used for converting row/column data when using the
    :meth:`~pgdumplib.dump.Dump.read_data` iterator.

    This class just splits the row into individual columns and returns
    the row as tuple of strings, only converting `\\N` to `None`.

    """
    def convert(self, row):
        """Convert the string based row into a tuple of columns.

        :param str row: The row to convert
        :rtype: tuple

        """
        return tuple(None if e == '\\N' else e for e in row.split('\t'))


class SmartDataConverter(DataConverter):
    """Attempts to convert columns to native Python data types

    Possible conversion types:

    - int
    - datetime.datetime
    - decimal.Decimal
    - ipaddress.IPv4Address
    - ipaddress.IPv4Network
    - ipaddress.IPv6Address
    - ipaddress.IPv6Network
    - None
    - str
    - uuid.UUID

    """
    def convert(self, row):
        """Convert the string based row into a tuple of columns.

        :param str row: The row to convert
        :rtype: tuple

        """
        return tuple(self._convert_column(c) for c in row.split('\t'))

    @staticmethod
    def _convert_column(column):
        """Attempt to convert the column from a string if appropriate

        :param str column: The column to attempt to convert
        :rtype: mixed

        """
        if column == '\\N':
            return None
        if column.strip('-').isnumeric():
            try:
                return int(column)
            except ValueError:
                pass
        elif column.strip('-').replace('.', '').isnumeric():
            try:
                return decimal.Decimal(column)
            except (ValueError, decimal.InvalidOperation):
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
            return uuid.UUID(column)
        except ValueError:
            pass
        try:
            return arrow.get(column).datetime
        except parser.ParserError:
            pass
        return column
