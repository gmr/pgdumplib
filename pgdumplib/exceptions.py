"""
pgdumplib specific exceptions

"""


class PgDumpLibException(Exception):
    """Common Base Exception"""


class NoDataError(PgDumpLibException):
    """Raised when attempting to work with data when do data entries exist"""


class EntityNotFoundError(PgDumpLibException):
    """Raised when an attempt is made to read data from a relation in a
    dump file but it is not found in the table of contents.

    This can happen if a schema-only dump was created OR if the ``namespace``
    and ``table`` specified were not found.

    """
    def __init__(self, namespace, table, *args):
        super().__init__(args)
        self.namespace = namespace
        self.table = table

    def __repr__(self):  # pragma: nocover
        return '<EntityNotFound namespace={!r} table={!r}>'.format(
            self.namespace, self.table)

    def __str__(self):  # pragma: nocover
        return 'Did not find {}.{} in the table of contents'.format(
            self.namespace, self.table)
