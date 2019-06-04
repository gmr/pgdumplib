"""
pg_dump Constants
=================

"""
MAGIC = 'PGDMP'
MIN_VER = (1, 12, 0)
FORMATS = ['Unknown', 'Custom', 'Files', 'Tar', 'Null', 'Directory']
SECTIONS = ['None', 'Pre-Data', 'Data', 'Post-Data']

BLK_DATA = b'\x01'
BLK_BLOBS = b'\x03'

EOF = -1

K_OFFSET_POS_NOT_SET = 1
K_OFFSET_POS_SET = 2
K_OFFSET_NO_DATA = 3

LOBBUFSIZE = 16384
