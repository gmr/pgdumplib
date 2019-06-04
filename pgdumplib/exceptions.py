"""
pgdumplib specific exceptions

"""
class PgDumpLibException(Exception):
    """Common Base Exception"""


class EntityNotFoundError(PgDumpLibException):
    """Raised when an attempt is made to read data from a relation in a
    dump file but it is not found in the table of contents.

    """
    def __init__(self, namespace, table, *args):
        super().__init__(args)
        self.namespace = namespace
        self.table = table

    def __repr__(self):
        return '<EntityNotFound namespace={!r} table={!r}>'.format(
            self.namespace, self.table)

    def __str__(self):
        return 'Did not find {}.{} in the table of contents'.format(
            self.namespace, self.table)
