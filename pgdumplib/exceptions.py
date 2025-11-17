"""
pgdumplib specific exceptions

"""


class PgDumpLibError(Exception):
    """Common Base Exception"""


class NoDataError(PgDumpLibError):
    """Raised when attempting to work with data when do data entries exist"""


class EntityNotFoundError(PgDumpLibError):
    """Raised when an attempt is made to read data from a relation in a
    dump file but it is not found in the table of contents.

    This can happen if a schema-only dump was created OR if the ``namespace``
    and ``table`` specified were not found.

    """

    def __init__(self, namespace: str, table: str):
        super().__init__()
        self.namespace = namespace
        self.table = table

    def __repr__(self) -> str:  # pragma: nocover
        return (
            f'<EntityNotFound namespace={self.namespace!r} '
            f'table={self.table!r}>'
        )

    def __str__(self) -> str:  # pragma: nocover
        return (
            f'Did not find {self.namespace}.{self.table} in the table '
            f'of contents'
        )
