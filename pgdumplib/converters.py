"""
When creating a new :class:`pgdumplib.dump.Dump` instance, either directly
or by using :py:func:`pgdumplib.load`, you can specify a converter class to
use when reading data using the :meth:`pgdumplib.dump.Dump.read_data` iterator.

The default converter (:py:class:`DataConverter`) handles PostgreSQL COPY
text format escape sequences and replaces columns that have a ``NULL``
indicator (``\\N``) with :py:const:`None`.

Supported escape sequences:
    - ``\\b`` - backspace
    - ``\\f`` - form feed
    - ``\\n`` - newline
    - ``\\r`` - carriage return
    - ``\\t`` - tab
    - ``\\v`` - vertical tab
    - ``\\NNN`` - octal byte value (1-3 digits)
    - ``\\xNN`` - hex byte value (1-2 digits)
    - ``\\\\`` - backslash
    - ``\\X`` - any other character X taken literally

The :py:class:`SmartDataConverter` extends the base converter and will
attempt to convert individual columns to native Python data types after
unescaping.

Creating your own data converter is easy and should simply extend the
:py:class:`DataConverter` class.

"""

import datetime
import decimal
import ipaddress
import typing
import uuid

import pendulum


def unescape_copy_text(field: str) -> str:  # noqa: C901
    """Unescape PostgreSQL COPY text format escape sequences.

    This function implements the same escape sequence handling as PostgreSQL's
    CopyReadAttributesText() function in copyfromparse.c.

    Supported escape sequences:
        \\b - backspace (ASCII 8)
        \\f - form feed (ASCII 12)
        \\n - newline (ASCII 10)
        \\r - carriage return (ASCII 13)
        \\t - tab (ASCII 9)
        \\v - vertical tab (ASCII 11)
        \\NNN - octal byte value (1-3 digits)
        \\xNN - hex byte value (1-2 digits)
        \\X - any other character X literally

    :param field: The escaped field string
    :return: The unescaped string

    """
    if '\\' not in field:
        return field

    result = []
    i = 0
    while i < len(field):
        if field[i] == '\\' and i + 1 < len(field):
            i += 1
            c = field[i]

            # Octal escape: \0-\7
            if '0' <= c <= '7':
                val = ord(c) - ord('0')
                # Check for second octal digit
                if i + 1 < len(field) and '0' <= field[i + 1] <= '7':
                    i += 1
                    val = (val << 3) + (ord(field[i]) - ord('0'))
                    # Check for third octal digit
                    if i + 1 < len(field) and '0' <= field[i + 1] <= '7':
                        i += 1
                        val = (val << 3) + (ord(field[i]) - ord('0'))
                result.append(chr(val & 0o377))

            # Hex escape: \xNN
            elif c == 'x':
                if i + 1 < len(field):
                    hex_chars = field[i + 1 : i + 3]
                    hex_val = ''
                    for hc in hex_chars:
                        if hc in '0123456789abcdefABCDEF':
                            hex_val += hc
                        else:
                            break
                    if hex_val:
                        i += len(hex_val)
                        result.append(chr(int(hex_val, 16) & 0xFF))
                    else:
                        result.append(c)
                else:
                    result.append(c)

            # Single character escapes
            elif c == 'b':
                result.append('\b')
            elif c == 'f':
                result.append('\f')
            elif c == 'n':
                result.append('\n')
            elif c == 'r':
                result.append('\r')
            elif c == 't':
                result.append('\t')
            elif c == 'v':
                result.append('\v')
            else:
                # Any other backslashed character is literal
                result.append(c)

            i += 1
        else:
            result.append(field[i])
            i += 1

    return ''.join(result)


class DataConverter:
    """Base Row/Column Converter

    Base class used for converting row/column data when using the
    :meth:`pgdumplib.dump.Dump.read_data` iterator.

    This class splits the row into individual columns, unescapes
    PostgreSQL COPY text format escape sequences, and converts ``\\N``
    to :py:const:`None`.

    """

    @staticmethod
    def convert(row: str) -> tuple[typing.Any, ...]:
        """Convert the string based row into a tuple of columns.

        :param str row: The row to convert
        :rtype: tuple

        """
        return tuple(
            None if e == '\\N' else unescape_copy_text(e)
            for e in row.split('\t')
        )


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

    @staticmethod
    def convert(row: str) -> tuple[SmartColumn, ...]:
        """Convert the string based row into a tuple of columns"""
        return tuple(
            SmartDataConverter._convert_column(c) for c in row.split('\t')
        )

    @staticmethod
    def _convert_column(column: str) -> SmartColumn:
        """Attempt to convert the column from a string if appropriate"""
        # Check for NULL before unescaping (PostgreSQL does this)
        if column == '\\N':
            return None
        # Unescape the column data
        column = unescape_copy_text(column)
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
