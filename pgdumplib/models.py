import dataclasses

from pgdumplib import constants


@dataclasses.dataclass(eq=True)
class Entry:
    """The entry model represents a single entry in the dataclass

    Custom formatted dump files are primarily comprised of entries, which
    contain all of the metadata and DDL required to construct the database.

    For table data and blobs, there are entries that contain offset locations
    in the dump file that instruct the reader as to where the data lives
    in the file.

    :var dump_id: The dump id, will be auto-calculated if left empty
    :var had_dumper: Indicates
    :var oid: The OID of the object the entry represents
    :var tag: The name/table/relation/etc of the entry
    :var desc: The entry description
    :var defn: The DDL definition for the entry
    :var drop_stmt: A drop statement used to drop the entry before
    :var copy_stmt: A copy statement used when there is a corresponding
        data section.
    :var namespace: The namespace of the entry
    :var tablespace: The tablespace to use
    :var tableam: The table access method
    :var owner: The owner of the object in Postgres
    :var with_oids: Indicates ...
    :var dependencies: A list of dump_ids of objects that the entry
        is dependent upon.
    :var data_state: Indicates if the entry has data and how it is stored
    :var offset: If the entry has data, the offset to the data in the file
    :var section: The section of the dump file the entry belongs to

    """
    dump_id: int
    had_dumper: bool = False
    table_oid: str = '0'
    oid: str = '0'
    tag: str | None = None
    desc: str = 'Unknown'
    defn: str | None = None
    drop_stmt: str | None = None
    copy_stmt: str | None = None
    namespace: str | None = None
    tablespace: str | None = None
    tableam: str | None = None
    owner: str | None = None
    with_oids: bool = False
    dependencies: list[int] = dataclasses.field(default_factory=list)
    data_state: int = constants.K_OFFSET_NO_DATA
    offset: int = 0

    @property
    def section(self) -> str:
        """Return the section the entry belongs to"""
        return constants.SECTION_MAPPING[self.desc]
