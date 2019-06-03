# coding=utf-8
"""
Directory
=========
Implements the Dump class for Directory based

"""
import io
import pathlib

from pgdumplib import constants, models, reader


class ToC(reader.ToC):
    """Implements the Table of Contents with the changes needed for the
    custom file format.

    """
    def _read_entry(self):
        """Read in an individual entry.

        :rtype: pgdumplib.models.Entry

        """
        args = [
            self._read_int(),  # Dump ID
            bool(self._read_int()),  # Has Dumper
            self._read_bytes().decode('utf-8'),  # Table OID
            self._read_bytes().decode('utf-8'),  # OID
            self._read_bytes().decode('utf-8'),  # Tag
            self._read_bytes().decode('utf-8'),  # Desc
            constants.SECTIONS[self._read_int() - 1],  # Section
            self._read_bytes().decode('utf-8'),  # Definition
            self._read_bytes().decode('utf-8'),  # Drop Statement
            self._read_bytes().decode('utf-8'),  # Copy Statement
            self._read_bytes().decode('utf-8'),  # Namespace
            self._read_bytes().decode('utf-8'),  # Tablespace
            self._read_bytes().decode('utf-8'),  # Owner
            self._read_bytes() == b'true',  # With OIDs
            self._read_dependencies(),  # Dependencies
            self.handle.tell(),  # Current Position
            self._read_int()]  # Number of bytes with data
        if args[-1] > 0:
            self.handle.seek(args[-1], io.SEEK_CUR)
        args.append(self._read_uint())  # Extra data
        return models.Entry(*args)


def load(filepath):
    """Load a pg_dump file created with -Fd

    :raises: `ValueError`
    :rtype: pgdumplib.reader.Dump

    """
    path = pathlib.Path(filepath)
    if not path.exists():
        raise ValueError('Path {!r} does not exist'.format(path))

    with open(path, 'rb') as handle:
        return reader.Dump(str(path), ToC(handle).read())
