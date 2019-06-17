"""
Constants used in the reading and writing of a :command:`pg_dump` file.
There are additional undocumented constants, but they should not be of concern
unless you are hacking on the library itself.

"""
import typing

APPEAR_AS: str = '11.3'
"""Version of PostgreSQL to appear as"""

BLK_DATA: bytes = b'\x01'
BLK_BLOBS: bytes = b'\x03'

BLOBS: str = 'BLOBS'
ENCODING: str = 'ENCODING'

EOF: int = -1

FORMAT_UNKNOWN: int = 0
FORMAT_CUSTOM: int = 1
FORMAT_FILES: int = 2
FORMAT_TAR: int = 3
FORMAT_NULL: int = 4
FORMAT_DIRECTORY: int = 5

FORMATS: typing.List[str] = [
    'Unknown',
    'Custom',
    'Files',
    'Tar',
    'Null',
    'Directory'
]

K_OFFSET_POS_NOT_SET: int = 1
"""Specifies the entry has data but no offset"""
K_OFFSET_POS_SET: int = 2
"""Specifies the entry has data and an offset"""
K_OFFSET_NO_DATA: int = 3
"""Specifies the entry has no data"""

MAGIC: bytes = b'PGDMP'

MIN_VER: typing.Tuple[int, int, int] = (1, 12, 0)
"""The minumum supported version of pg_dump files ot support"""

MAX_VER: typing.Tuple[int, int, int] = (1, 13, 0)
"""The maximum supported version of pg_dump files ot support"""

PGDUMP_STRFTIME_FMT: str = '%Y-%m-%d %H:%M:%S %Z'

SECTION_NONE: str = 'None'
"""Non-specific section for an entry in a dump's table of contents"""

SECTION_PRE_DATA: str = 'Pre-Data'
"""Pre-data section for an entry in a dump's table of contents"""

SECTION_DATA: str = 'DATA'
"""Data section for an entry in a dump's table of contents"""

SECTION_POST_DATA: str = 'Post-Data'
"""Post-data section for an entry in a dump's table of contents"""

SECTIONS: typing.List[str] = [
    SECTION_NONE,
    SECTION_PRE_DATA,
    SECTION_DATA,
    SECTION_POST_DATA
]

SECTION_SORT: typing.Dict[str, int] = {
    SECTION_NONE: 0,
    SECTION_PRE_DATA: 1,
    SECTION_DATA: 2,
    SECTION_POST_DATA: 3
}

TABLE_DATA: str = 'TABLE DATA'

VERSION: typing.Tuple[int, int, int] = (1, 13, 0)
"""pg_dump file format version to create by default"""

ZLIB_OUT_SIZE: int = 4096
ZLIB_IN_SIZE: int = 4096
