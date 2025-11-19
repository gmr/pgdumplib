import datetime
import ipaddress
import unittest
import uuid

import faker
import maya

from pgdumplib import constants, converters


class TestCase(unittest.TestCase):
    def test_data_converter(self):
        data = []
        for row in range(0, 10):
            data.append(
                [
                    str(row),
                    str(uuid.uuid4()),
                    str(datetime.datetime.now(tz=datetime.UTC)),
                    str(uuid.uuid4()),
                    None,
                ]
            )

        converter = converters.DataConverter()
        for _offset, expectation in enumerate(data):
            line = '\t'.join(['\\N' if e is None else e for e in expectation])
            self.assertListEqual(list(converter.convert(line)), expectation)

    def test_noop_converter(self):
        converter = converters.NoOpConverter()
        value = '1\t\\N\tfoo\t     \t'
        self.assertEqual(converter.convert(value), value)

    def test_smart_data_converter(self):
        def convert(value):
            """Convert the value to the proper string type"""
            if value is None:
                return '\\N'
            elif isinstance(value, datetime.datetime):
                return value.strftime(constants.PGDUMP_STRFTIME_FMT)
            return str(value)

        fake = faker.Faker()
        data = []
        for row in range(0, 10):
            data.append(
                [
                    row,
                    None,
                    fake.pydecimal(
                        positive=True, left_digits=5, right_digits=3
                    ),
                    uuid.uuid4(),
                    ipaddress.IPv4Network(fake.ipv4(True)),
                    ipaddress.IPv4Address(fake.ipv4()),
                    ipaddress.IPv6Address(fake.ipv6()),
                    maya.now()
                    .datetime(to_timezone='US/Eastern', naive=True)
                    .strftime(constants.PGDUMP_STRFTIME_FMT),
                ]
            )

        converter = converters.SmartDataConverter()
        for _offset, expectation in enumerate(data):
            line = '\t'.join([convert(e) for e in expectation])
            row = list(converter.convert(line))
            self.assertListEqual(row, expectation)

    def test_smart_data_converter_bad_date(self):
        converter = converters.SmartDataConverter()
        row = '2019-13-45 25:34:99 00:00\t1\tfoo\t\\N'
        self.assertEqual(
            converter.convert(row),
            ('2019-13-45 25:34:99 00:00', 1, 'foo', None),
        )


class UnescapeCopyTextTestCase(unittest.TestCase):
    """Test cases for PostgreSQL COPY text format escape sequence handling.

    These tests verify that pgdumplib correctly handles all escape sequences
    as defined in the PostgreSQL COPY text format specification, matching
    the behavior of PostgreSQL's CopyReadAttributesText() function.
    """

    def test_no_escapes(self):
        """Test that strings without escapes pass through unchanged"""
        self.assertEqual(converters.unescape_copy_text('hello'), 'hello')
        self.assertEqual(
            converters.unescape_copy_text('test data'), 'test data'
        )
        self.assertEqual(converters.unescape_copy_text(''), '')

    def test_backslash_escape(self):
        """Test that backslash itself can be escaped"""
        self.assertEqual(converters.unescape_copy_text('\\\\'), '\\')
        self.assertEqual(converters.unescape_copy_text('a\\\\b'), 'a\\b')
        self.assertEqual(converters.unescape_copy_text('\\\\\\\\'), '\\\\')

    def test_newline_escape(self):
        """Test \\n escape sequence"""
        self.assertEqual(converters.unescape_copy_text('\\n'), '\n')
        self.assertEqual(
            converters.unescape_copy_text('line1\\nline2'), 'line1\nline2'
        )
        self.assertEqual(converters.unescape_copy_text('a\\nb\\nc'), 'a\nb\nc')

    def test_carriage_return_escape(self):
        """Test \\r escape sequence"""
        self.assertEqual(converters.unescape_copy_text('\\r'), '\r')
        self.assertEqual(converters.unescape_copy_text('test\\r'), 'test\r')

    def test_tab_escape(self):
        """Test \\t escape sequence"""
        self.assertEqual(converters.unescape_copy_text('\\t'), '\t')
        self.assertEqual(converters.unescape_copy_text('a\\tb'), 'a\tb')

    def test_backspace_escape(self):
        """Test \\b escape sequence"""
        self.assertEqual(converters.unescape_copy_text('\\b'), '\b')
        self.assertEqual(converters.unescape_copy_text('test\\b'), 'test\b')

    def test_form_feed_escape(self):
        """Test \\f escape sequence"""
        self.assertEqual(converters.unescape_copy_text('\\f'), '\f')
        self.assertEqual(converters.unescape_copy_text('test\\f'), 'test\f')

    def test_vertical_tab_escape(self):
        """Test \\v escape sequence"""
        self.assertEqual(converters.unescape_copy_text('\\v'), '\v')
        self.assertEqual(converters.unescape_copy_text('test\\v'), 'test\v')

    def test_octal_escapes(self):
        """Test octal escape sequences \\NNN"""
        # Single digit octal
        self.assertEqual(converters.unescape_copy_text('\\0'), '\x00')
        self.assertEqual(converters.unescape_copy_text('\\7'), '\x07')

        # Two digit octal
        self.assertEqual(converters.unescape_copy_text('\\10'), '\x08')
        self.assertEqual(converters.unescape_copy_text('\\77'), '\x3f')

        # Three digit octal
        self.assertEqual(converters.unescape_copy_text('\\101'), 'A')
        self.assertEqual(converters.unescape_copy_text('\\141'), 'a')
        self.assertEqual(converters.unescape_copy_text('\\377'), '\xff')

        # Octal in context
        self.assertEqual(
            converters.unescape_copy_text('test\\101data'), 'testAdata'
        )

    def test_hex_escapes(self):
        """Test hex escape sequences \\xNN"""
        # Single digit hex
        self.assertEqual(converters.unescape_copy_text('\\x0'), '\x00')
        self.assertEqual(converters.unescape_copy_text('\\xA'), '\x0a')
        self.assertEqual(converters.unescape_copy_text('\\xa'), '\x0a')

        # Two digit hex
        self.assertEqual(converters.unescape_copy_text('\\x00'), '\x00')
        self.assertEqual(converters.unescape_copy_text('\\x41'), 'A')
        self.assertEqual(converters.unescape_copy_text('\\xFF'), '\xff')
        self.assertEqual(converters.unescape_copy_text('\\xff'), '\xff')

        # Hex in context
        self.assertEqual(
            converters.unescape_copy_text('test\\x41data'), 'testAdata'
        )

    def test_literal_escapes(self):
        """Test that unknown escape sequences are taken literally"""
        self.assertEqual(converters.unescape_copy_text('\\z'), 'z')
        self.assertEqual(converters.unescape_copy_text('\\@'), '@')
        self.assertEqual(converters.unescape_copy_text('\\?'), '?')

    def test_incomplete_hex_escapes(self):
        """Test edge cases with incomplete or invalid hex escapes"""
        # \x with no digits - 'x' is taken literally
        self.assertEqual(converters.unescape_copy_text('\\x'), 'x')
        # \x with non-hex character - 'x' and following char are literal
        self.assertEqual(converters.unescape_copy_text('\\xG'), 'xG')
        self.assertEqual(converters.unescape_copy_text('\\xZ9'), 'xZ9')

    def test_trailing_backslash(self):
        """Test that a trailing backslash is preserved"""
        self.assertEqual(converters.unescape_copy_text('foo\\'), 'foo\\')
        self.assertEqual(converters.unescape_copy_text('\\'), '\\')

    def test_combined_escapes(self):
        """Test multiple escape sequences in one string"""
        self.assertEqual(
            converters.unescape_copy_text('a\\tb\\nc\\\\d'),
            'a\tb\nc\\d',
        )
        self.assertEqual(
            converters.unescape_copy_text('\\x41\\x42\\x43'), 'ABC'
        )
        self.assertEqual(
            converters.unescape_copy_text('\\101\\102\\103'), 'ABC'
        )

    def test_null_marker_not_unescaped(self):
        """Test that \\N is NOT unescaped by this function.

        The NULL marker \\N must be checked before calling unescape_copy_text,
        as PostgreSQL does in CopyReadAttributesText().
        """
        # unescape_copy_text should treat \N as literal N
        self.assertEqual(converters.unescape_copy_text('\\N'), 'N')

    def test_converter_preserves_null(self):
        """Test that DataConverter checks for \\N before unescaping"""
        converter = converters.DataConverter()
        # Use actual tab character as separator, \\N as NULL marker
        row = 'value1\tvalue2\t\\N\tvalue3'
        result = converter.convert(row)
        # The \\N should become None, not be unescaped to 'N'
        self.assertEqual(result[2], None)

    def test_converter_unescapes_data(self):
        """Test that DataConverter properly unescapes field data"""
        converter = converters.DataConverter()

        # Test backslash
        row = 'C:\\\\Windows\\\\System'
        result = converter.convert(row)
        self.assertEqual(result[0], 'C:\\Windows\\System')

        # Test newline in data
        row = 'line1\\nline2'
        result = converter.convert(row)
        self.assertEqual(result[0], 'line1\nline2')

        # Test tab-separated values with escaped tabs in data
        row = 'col1\\tdata\tcol2'
        result = converter.convert(row)
        self.assertEqual(result[0], 'col1\tdata')
        self.assertEqual(result[1], 'col2')

    def test_smart_converter_unescapes_before_type_conversion(self):
        """Test that SmartDataConverter unescapes before type detection"""
        converter = converters.SmartDataConverter()

        # Test that escaped characters don't interfere with type detection
        # Use actual tab as field separator
        row = '42\t\\N\thello\\nworld'
        result = converter.convert(row)
        self.assertEqual(result[0], 42)  # int
        self.assertEqual(result[1], None)  # NULL
        self.assertEqual(result[2], 'hello\nworld')  # string with newline
