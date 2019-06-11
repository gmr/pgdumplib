"""
Models
======
The models represent the different data structures in a pg_dump file

"""
import dataclasses
import typing

from pgdumplib import constants


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
