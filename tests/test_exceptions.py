import unittest

from pgdumplib import exceptions


class ExceptionTestCase(unittest.TestCase):

    def test_repr_formatting(self):
        exc = exceptions.EntityNotFoundError('public', 'table')
        self.assertEqual(
            repr(exc), "<EntityNotFound namespace='public' table='table'>")

    def test_str_formatting(self):
        exc = exceptions.EntityNotFoundError('public', 'table')
        self.assertEqual(
            str(exc), 'Did not find public.table in the table of contents')
