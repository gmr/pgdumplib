"""
When creating a new :class:`pgdumplib.dump.Dump` instance, either directly
or by using :py:func:`pgdumplib.load`, you can specify a converter class to
use when reading data using the :meth:`pgdumplib.dump.Dump.read_data` iterator.

The default converter (:py:class:`DataConverter`) will only replace columns
that have a ``NULL`` indicator (``\\N``) with :py:const:`None`.

The :py:class:`SmartDataConverter` will attempt to convert individual
columns to native Python data types.

Creating your own data converter is easy and should simply extend the
:py:class:`DataConverter` class.

"""

import datetime
import decimal
import ipaddress
import uuid

import pendulum


class DataConverter:
    """Base Row/Column Converter

    Base class used for converting row/column data when using the
    :meth:`pgdumplib.dump.Dump.read_data` iterator.

    This class just splits the row into individual columns and returns
    the row as tuple of strings, only converting ``\\N`` to :py:const:`None`.

    """

    @staticmethod
    def convert(row: str) -> tuple[str | None, ...]:
        """Convert the string based row into a tuple of columns.

        :param str row: The row to convert
        :rtype: tuple

        """
        return tuple(None if e == '\\N' else str(e) for e in row.split('\t'))


class NoOpConverter:
    """Performs no conversion on the row passed in"""

    @staticmethod
    def convert(row: str) -> str:
        """Returns the row passed in

        :param str row: The row to convert
        :rtype: str

        """
        return row


SmartColumn = (
    None
    | str
    | int
    | datetime.datetime
    | decimal.Decimal
    | ipaddress.IPv4Address
    | ipaddress.IPv4Network
    | ipaddress.IPv6Address
    | ipaddress.IPv6Network
    | uuid.UUID
)


class SmartDataConverter(DataConverter):
    """Attempts to convert columns to native Python data types

    Used for converting row/column data with the
    :meth:`pgdumplib.dump.Dump.read_data` iterator.

    Possible conversion types:

        - :py:class:`int`
        - :py:class:`datetime.datetime`
        - :py:class:`decimal.Decimal`
        - :py:class:`ipaddress.IPv4Address`
        - :py:class:`ipaddress.IPv4Network`
        - :py:class:`ipaddress.IPv6Address`
        - :py:class:`ipaddress.IPv6Network`
        - :py:const:`None`
        - :py:class:`str`
        - :py:class:`uuid.UUID`

    """

    def convert(self, row: str) -> tuple[SmartColumn, ...]:
        """Convert the string based row into a tuple of columns"""
        return tuple(self._convert_column(c) for c in row.split('\t'))

    @staticmethod
    def _convert_column(column: str) -> SmartColumn:
        """Attempt to convert the column from a string if appropriate"""
        if column == '\\N':
            return None
        if column.strip('-').isnumeric():
            return int(column)
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
        for tz_fmt in {'Z', 'ZZ', 'z', 'zz'}:
            for micro_fmt in {'.SSSSSS', ''}:
                try:
                    pdt = pendulum.from_format(
                        column, f'YYYY-MM-DD HH:mm:ss{micro_fmt} {tz_fmt}'
                    )
                    # Convert pendulum DateTime to datetime.datetime
                    return datetime.datetime.fromisoformat(pdt.isoformat())
                except ValueError:
                    pass
        return column
