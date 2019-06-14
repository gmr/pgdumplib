"""
Constants used in the reading and writing of a :command:`pg_dump` file.
There are additional undocumented constants, but they should not be of concern
unless you are hacking on the library itself.

"""
APPEAR_AS = '11.3'
"""Version of PostgreSQL to appear as"""

BLK_DATA = b'\x01'
BLK_BLOBS = b'\x03'

BLOBS = 'BLOBS'
ENCODING = 'ENCODING'

EOF = -1

FORMAT_UNKNOWN = 0
FORMAT_CUSTOM = 1
FORMAT_FILES = 2
FORMAT_TAR = 3
FORMAT_NULL = 4
FORMAT_DIRECTORY = 5

FORMATS = ['Unknown', 'Custom', 'Files', 'Tar', 'Null', 'Directory']

K_OFFSET_POS_NOT_SET = 1
"""Specifies the entry has data but no offset"""
K_OFFSET_POS_SET = 2
"""Specifies the entry has data and an offset"""
K_OFFSET_NO_DATA = 3
"""Specifies the entry has no data"""

MAGIC = b'PGDMP'

MIN_VER = (1, 12, 0)
"""The minumum supported version of pg_dump files ot support"""

MAX_VER = (1, 13, 0)
"""The maximum supported version of pg_dump files ot support"""

PGDUMP_STRFTIME_FMT = '%Y-%m-%d %H:%M:%S %Z'

SECTION_NONE = 'None'
"""Non-specific section for an entry in a dump's table of contents"""

SECTION_PRE_DATA = 'Pre-Data'
"""Pre-data section for an entry in a dump's table of contents"""

SECTION_DATA = 'DATA'
"""Data section for an entry in a dump's table of contents"""

SECTION_POST_DATA = 'Post-Data'
"""Post-data section for an entry in a dump's table of contents"""

SECTIONS = [
    SECTION_NONE,
    SECTION_PRE_DATA,
    SECTION_DATA,
    SECTION_POST_DATA
]

SECTION_SORT = {
    SECTION_NONE: 0,
    SECTION_PRE_DATA: 1,
    SECTION_DATA: 2,
    SECTION_POST_DATA: 3
}

TABLE_DATA = 'TABLE DATA'

VERSION = (1, 13, 0)
"""pg_dump file format version to create by default"""

ZLIB_OUT_SIZE = 4096
ZLIB_IN_SIZE = 4096
