"""
pg_dump Constants
=================

"""
MAGIC = 'PGDMP'
MIN_VER = (1, 12, 0)
FORMATS = ['Unknown', 'Custom', 'Files', 'Tar', 'Null', 'Directory']
SECTIONS = ['None', 'Pre-Data', 'Data', 'Post-Data']
