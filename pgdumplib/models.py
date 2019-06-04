"""
Models
======
The models represent the different data structures in a pg_dump file

"""
import dataclasses
import datetime


@dataclasses.dataclass
class Entry:
    """Represents a single entry in the dataclass"""
    dump_id: int
    had_dumper: bool
    table_oid: str
    oid: str
    tag: str
    desc: str
    section: str
    defn: str
    drop_stmt: str
    copy_stmt: str
    namespace: str
    tablespace: str
    owner: str
    with_oids: bool
    dependencies: list
    data_state: int
    offset: int


@dataclasses.dataclass
class Header:
    """Represents the pg_dump archive header"""
    vmaj: int
    vmin: int
    vrev: int
    intsize: int
    offsize: int
    format: str

    @property
    def version(self):
        """Return the version as a tuple to make version comparisons easier.

        :rtype: tuple

        """
        return self.vmaj, self.vmin, self.vrev


@dataclasses.dataclass
class ToC:
    """Represents the Table of Contents"""
    header: Header
    compression: bool
    timestamp: datetime.datetime
    dbname: str
    server_version: str
    dump_version: str
    entries: list
