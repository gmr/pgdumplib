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
