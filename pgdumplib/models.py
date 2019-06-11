"""
Models
======
The models represent the different data structures in a pg_dump file

"""
import dataclasses
import datetime
import typing

import arrow

from pgdumplib import __version__, constants


@dataclasses.dataclass
class Entry:
    """Represents a single entry in the dataclass"""
    dump_id: int
    had_dumper: bool = False
    table_oid: typing.Optional[str] = None
    oid: typing.Optional[str] = None
    tag: typing.Optional[str] = None
    desc: typing.Optional[str] = None
    section: str = constants.SECTIONS[0]
    defn: typing.Optional[str] = None
    drop_stmt: typing.Optional[str] = None
    copy_stmt: typing.Optional[str] = None
    namespace: typing.Optional[str] = None
    tablespace: typing.Optional[str] = None
    owner: typing.Optional[str] = None
    with_oids: bool = False
    dependencies: list = dataclasses.field(default_factory=list)
    data_state: int = constants.K_OFFSET_NO_DATA
    offset: int = 0


@dataclasses.dataclass
class Header:
    """Represents the pg_dump archive header"""
    vmaj: int = constants.MIN_VER[0]
    vmin: int = constants.MIN_VER[1]
    vrev: int = constants.MIN_VER[2]
    intsize: int = 4
    offsize: int = 8
    format: str = 'Custom'

    @property
    def version(self):
        """Return the version as a tuple to make version comparisons easier.

        :rtype: tuple

        """
        return self.vmaj, self.vmin, self.vrev


@dataclasses.dataclass
class ToC:
    """Represents the Table of Contents"""
    header: Header = dataclasses.field(default_factory=Header)
    compression: bool = False
    timestamp: datetime.datetime = dataclasses.field(
        default_factory=arrow.now)
    dbname: str = 'pgdumplib'
    server_version: str = '{} (pgdumplib {})'.format(
        constants.APPEAR_AS, __version__.version)
    dump_version: str = '{} (pgdumplib {})'.format(
        constants.APPEAR_AS, __version__.version)
    entries: list = dataclasses.field(default_factory=list)
