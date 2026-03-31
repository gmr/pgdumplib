"""
The :py:class:`~pgdumplib.dump.Dump` class exposes methods to
:py:meth:`load <pgdumplib.dump.Dump.load>` an existing dump,
to :py:meth:`add an entry <pgdumplib.dump.Dump.add_entry>` to a dump,
to :py:meth:`add table data <pgdumplib.dump.Dump.add_data>` to a dump,
to :py:meth:`add blob data <pgdumplib.dump.Dump.add_blob>` to a dump,
and to :py:meth:`save <pgdumplib.dump.Dump.save>` a new dump.

There are :doc:`converters` that are available to format the data that is
returned by :py:meth:`~pgdumplib.dump.Dump.read_data`. The converter
is passed in during construction of a new :py:class:`~pgdumplib.dump.Dump`,
and is also available as an argument to :py:func:`pgdumplib.load`.

The default converter, :py:class:`~pgdumplib.converters.DataConverter` will
return all fields as strings, only replacing ``NULL`` with
:py:const:`None`. The :py:class:`~pgdumplib.converters.SmartDataConverter`
will attempt to convert all columns to native Python data types.

"""

import contextlib
import datetime
import io
import logging
import os
import pathlib
import re
import typing
from collections import abc

from pgdumplib import _pgdumplib, constants, converters, models

LOGGER = logging.getLogger(__name__)

ENCODING_PATTERN = re.compile(r"'(\w+)'")


class TableData:
    """Used to encapsulate table data allowing for the appending of
    data one row at a time.

    Do not create this class directly, instead invoke
    :py:meth:`~pgdumplib.dump.Dump.table_data_writer`.

    """

    def __init__(self, dump_id: int, encoding: str):
        self.dump_id = dump_id
        self._encoding = encoding
        self._buffer = io.BytesIO()

    def append(self, *args) -> None:
        """Append a row to the table data, passing columns in as args

        Column order must match the order specified when
        :py:meth:`~pgdumplib.dump.Dump.table_data_writer` was invoked.

        All columns will be coerced to a string with special attention
        paid to ``None``, converting it to the null marker (``\\N``) and
        :py:class:`datetime.datetime` objects, which will have the proper
        pg_dump timestamp format applied to them.

        """
        row = '\t'.join([self._convert(c) for c in args])
        self._buffer.write(f'{row}\n'.encode(self._encoding))

    def get_data(self) -> bytes:
        """Return the accumulated data as bytes"""
        return self._buffer.getvalue()

    @property
    def size(self) -> int:
        """Return the current size of the buffered data"""
        return self._buffer.tell()

    @staticmethod
    def _convert(column: typing.Any) -> str:
        """Convert the column to a string"""
        if isinstance(column, datetime.datetime):
            return column.strftime(constants.PGDUMP_STRFTIME_FMT)
        elif column is None:
            return '\\N'
        return str(column)


class Dump:
    """Create a new instance of the :py:class:`~pgdumplib.dump.Dump` class

    Once created, the instance of :py:class:`~pgdumplib.dump.Dump` can
    be used to read existing dumps or to create new ones.

    :param str dbname: The database name for the dump
        (Default: ``pgdumplib``)
    :param str encoding: The data encoding (Default: ``UTF8``)
    :param converter: The data converter class to use
        (Default: :py:class:`pgdumplib.converters.DataConverter`)
    :param str appear_as: The version of Postgres to emulate
        (Default: ``12.0``)

    """

    def __init__(
        self,
        dbname: str = 'pgdumplib',
        encoding: str = 'UTF8',
        converter: typing.Any = None,
        appear_as: str = '12.0',
    ):
        self._validate_appear_as(appear_as)
        self._dump = _pgdumplib.Dump(dbname, encoding, appear_as)
        self.encoding = encoding
        self._format = 'Custom'
        self._loaded_compression: str | None = None
        converter = converter or converters.DataConverter
        self._converter: converters.DataConverter = converter()

    def __repr__(self) -> str:
        return (
            f'<Dump format={self.format!r} '
            f'timestamp={self.timestamp.isoformat()!r} '
            f'entry_count={self._dump.entry_count()!r}>'
        )

    @property
    def compression_algorithm(self) -> str:
        """The compression algorithm used in the dump"""
        if self._loaded_compression is not None:
            return self._loaded_compression
        return self._dump.compression

    @property
    def dbname(self) -> str:
        """The database name"""
        return self._dump.dbname

    @property
    def dump_version(self) -> str:
        """The pg_dump version string"""
        return self._dump.dump_version

    @property
    def entries(self) -> list[models.Entry]:
        """The list of entries in the dump"""
        return [self._to_entry(e) for e in self._dump.entries]

    @property
    def format(self) -> str:
        """The dump format (Custom, Directory, Tar)"""
        return self._format

    @format.setter
    def format(self, value: str) -> None:
        self.set_format(value)

    @property
    def server_version(self) -> str:
        """The PostgreSQL server version string"""
        return self._dump.server_version

    @property
    def timestamp(self) -> datetime.datetime:
        """The dump creation timestamp"""
        return self._dump.timestamp

    @property
    def version(self) -> tuple[int, int, int]:
        """The archive format version as a tuple"""
        return self._dump.version

    def add_entry(
        self,
        desc: str,
        namespace: str | None = None,
        tag: str | None = None,
        owner: str | None = None,
        defn: str | None = None,
        drop_stmt: str | None = None,
        copy_stmt: str | None = None,
        dependencies: list[int] | None = None,
        tablespace: str | None = None,
        tableam: str | None = None,
        dump_id: int | None = None,
    ) -> models.Entry:
        """Add an entry to the dump

        A :py:exc:`ValueError` will be raised if `desc` is not value that
        is known in :py:module:`pgdumplib.constants`.

        When adding data, use :py:meth:`~Dump.table_data_writer` instead
        of invoking :py:meth:`~Dump.add_entry` directly.

        If ``dependencies`` are specified, they will be validated and if a
        ``dump_id`` is specified and no entry is found with that
        ``dump_id``, a :py:exc:`ValueError` will be raised.

        Other omitted values will be set to the default values specified
        in the :py:class:`pgdumplib.dump.Entry` class.

        The ``dump_id`` will be auto-calculated based upon the existing
        entries if it is not specified.

        .. note:: The creation of ad-hoc blobs is not supported.

        :param str desc: The entry description
        :param str namespace: The namespace of the entry
        :param str tag: The name/table/relation/etc of the entry
        :param str owner: The owner of the object in Postgres
        :param str defn: The DDL definition for the entry
        :param drop_stmt: A drop statement used to drop the entry
        :param copy_stmt: A copy statement used when there is a
            corresponding data section.
        :param list dependencies: A list of dump_ids of objects that
            the entry is dependent upon.
        :param str tablespace: The tablespace to use
        :param str tableam: The table access method
        :param int dump_id: The dump id, will be auto-calculated if
            left empty
        :raises: :py:exc:`ValueError`
        :rtype: pgdumplib.dump.Entry

        """
        if desc not in constants.SECTION_MAPPING:
            raise ValueError(f'Invalid desc: {desc}')

        if dump_id is not None and dump_id < 1:
            raise ValueError('dump_id must be greater than 1')

        # TODO: Pass dump_id through to _dump.add_entry once the Rust
        # backend supports it, then remove this guard.
        if dump_id is not None:
            raise NotImplementedError(
                'Custom dump_id is not yet supported by the Rust backend'
            )

        existing_ids = self._dump.entry_dump_ids()

        if dump_id and dump_id in existing_ids:
            raise ValueError(f'dump_id {dump_id!r} is already assigned')

        for dependency in dependencies or []:
            if dependency not in existing_ids:
                raise ValueError(
                    f'Dependency dump_id {dependency!r} not found'
                )

        new_id = self._dump.add_entry(
            desc,
            namespace=namespace,
            tag=tag,
            owner=owner,
            defn=defn,
            drop_stmt=drop_stmt,
            copy_stmt=copy_stmt,
            dependencies=dependencies,
        )
        if tableam or tablespace:
            self._dump.update_entry(
                new_id, tableam=tableam, tablespace=tablespace
            )
        return self._to_entry(self._dump.get_entry(new_id))

    def blobs(self) -> typing.Generator[tuple[int, bytes], None, None]:
        """Iterator that returns each blob in the dump

        :rtype: tuple(int, bytes)

        """
        yield from self._dump.blobs()

    def get_entry(self, dump_id: int) -> models.Entry | None:
        """Return the entry for the given `dump_id`

        :param int dump_id: The dump ID of the entry to return.

        """
        entry = self._dump.get_entry(dump_id)
        if entry is None:
            return None
        return self._to_entry(entry)

    def load(self, path: str | os.PathLike) -> typing.Self:
        """Load the Dumpfile

        .. note::
            Loaded dumps are saved without compression by default,
            regardless of the original compression setting. The
            original compression algorithm is preserved for
            inspection via :attr:`compression_algorithm`. Use
            :meth:`set_compression` after loading to specify the
            desired output compression.

        :param os.PathLike path: The path of the dump to load
        :raises: :py:exc:`RuntimeError`
        :raises: :py:exc:`ValueError`

        """
        path = pathlib.Path(path)
        try:
            if path.is_file():
                self._check_file_format(path)
            self._dump = _pgdumplib.Dump.load(str(path))
        except OSError as err:
            raise ValueError(str(err)) from err

        ver = self._dump.version
        if not constants.MIN_VER <= ver <= constants.MAX_VER:
            raise ValueError(
                'Unsupported backup version: {}.{}.{}'.format(*ver)
            )

        # Preserve original compression for reporting, but save with none
        self._loaded_compression = self._dump.compression
        self._dump.set_compression(constants.COMPRESSION_NONE)

        self._detect_encoding()
        return self

    def lookup_entry(
        self,
        desc: str,
        namespace: str,
        tag: str,
    ) -> models.Entry | None:
        """Lookup an entry by description, namespace, and tag

        :param str desc: The entry description
        :param str namespace: The namespace
        :param str tag: The tag
        :rtype: pgdumplib.models.Entry or None

        """
        if desc not in constants.SECTION_MAPPING:
            raise ValueError(f'Invalid desc: {desc}')
        entry = self._dump.lookup_entry(desc, namespace, tag)
        if entry is None:
            return None
        return self._to_entry(entry)

    def save(self, path: str | os.PathLike) -> None:
        """Save the dump to disk

        :param os.PathLike path: The path to save the dump to

        """
        self._dump.save(str(path))

    def set_compression(self, algorithm: str) -> None:
        """Set the compression algorithm

        :param str algorithm: The compression algorithm
            (none, gzip, lz4, zstd)

        """
        self._loaded_compression = None
        self._dump.set_compression(algorithm)

    def set_format(self, fmt: str) -> None:
        """Set the output format

        :param str fmt: The output format (Custom, Directory, Tar)

        """
        self._dump.set_format(fmt)
        self._format = fmt

    def table_data(
        self,
        namespace: str,
        table: str,
    ) -> typing.Generator:
        """Iterate over the data for the specified table

        :param str namespace: The namespace/schema
        :param str table: The table name
        :raises: :py:exc:`pgdumplib.exceptions.EntityNotFoundError`
        :raises: :py:exc:`pgdumplib.exceptions.NoDataError`

        """
        for row in self._dump.table_data(namespace, table):
            yield self._converter.convert(row)

    @contextlib.contextmanager
    def table_data_writer(
        self,
        entry: models.Entry,
        columns: abc.Sequence,
    ) -> typing.Generator[TableData, None, None]:
        """Context manager for writing table data

        :param entry: The entry the data is for
        :param columns: The columns being written
        :rtype: TableData

        """
        copy_stmt = (
            f'COPY {entry.namespace}.{entry.tag}'
            f' ({", ".join(columns)}) FROM stdin;\n'
        )
        # Reuse an existing TABLE DATA entry for this table if one
        # exists, avoiding orphan entries on repeated calls.
        existing = self._find_existing_table_data(
            entry.namespace,
            entry.tag,
        )
        if existing is not None:
            existing_entry = self._dump.get_entry(existing)
            if (
                existing_entry is not None
                and existing_entry.copy_stmt != copy_stmt
            ):
                raise ValueError(
                    'columns must match the existing TABLE DATA entry'
                )
            dump_id = existing
        else:
            dump_id = self._dump.add_entry(
                constants.TABLE_DATA,
                namespace=entry.namespace or None,
                tag=entry.tag or None,
                owner=entry.owner or None,
                copy_stmt=copy_stmt,
                dependencies=[entry.dump_id],
            )
        writer = TableData(dump_id, self.encoding)
        yield writer
        data = writer.get_data()
        if existing is not None:
            existing_data = self._dump.entry_data(existing)
            if existing_data:
                data = bytes(existing_data) + data
        self._dump.set_entry_data(dump_id, data)

    def _find_existing_table_data(
        self,
        namespace: str | None,
        tag: str | None,
    ) -> int | None:
        """Find an existing TABLE DATA entry for the same table"""
        for entry in self._dump.entries:
            if (
                entry.desc == constants.TABLE_DATA
                and entry.namespace == namespace
                and entry.tag == tag
            ):
                return entry.dump_id
        return None

    @staticmethod
    def _check_file_format(path: pathlib.Path) -> None:
        with open(path, 'rb') as handle:
            header = handle.read(16)

        if not header:
            raise ValueError(
                'Empty file. Use pg_dump -Fc to create a custom format dump.'
            )

        if header[:5] == constants.MAGIC:
            return

        try:
            text = header.decode('utf-8', errors='replace')
            if text.startswith(
                ('--', 'CREATE', 'SET', 'SELECT', 'INSERT', 'ALTER', 'DROP')
            ):
                raise ValueError(
                    'This appears to be a plain SQL text file, '
                    'not a custom format dump. '
                    'Use pg_dump -Fc to create a '
                    'custom format dump.'
                )
        except UnicodeDecodeError:
            pass

        # Allow unknown binary formats (e.g., tar archives) to pass
        # through to the Rust backend for handling

    def _detect_encoding(self) -> None:
        """Detect the encoding from the dump's ENCODING entry"""
        entry = self._dump.get_entry(1)
        if entry and entry.desc == constants.ENCODING and entry.defn:
            match = ENCODING_PATTERN.search(entry.defn)
            if match:
                self.encoding = match.group(1)

    @staticmethod
    def _validate_appear_as(appear_as: str) -> None:
        parts = appear_as.split('.')
        if len(parts) < 2:
            raise ValueError(f'Invalid appear_as version: {appear_as}')
        try:
            major = int(parts[0])
            minor = int(parts[1])
        except ValueError as err:
            raise ValueError(
                f'Invalid appear_as version: {appear_as}'
            ) from err

        for key_range, _value in constants.K_VERSION_MAP.items():
            pg_ver = (major, minor)
            if len(key_range[0]) == 3:
                pg_ver = (
                    major,
                    minor,
                    int(parts[2]) if len(parts) > 2 else 0,
                )
            if key_range[0] <= pg_ver <= key_range[1]:
                return
        raise RuntimeError(f'Unsupported version: {appear_as}')

    @staticmethod
    def _to_entry(raw: _pgdumplib.Entry) -> models.Entry:
        return models.Entry(
            dump_id=raw.dump_id,
            had_dumper=raw.had_dumper,
            table_oid=raw.table_oid or '0',
            oid=raw.oid or '0',
            tag=raw.tag,
            desc=raw.desc,
            defn=raw.defn,
            drop_stmt=raw.drop_stmt,
            copy_stmt=raw.copy_stmt,
            namespace=raw.namespace,
            tablespace=raw.tablespace,
            tableam=raw.tableam,
            relkind=raw.relkind,
            owner=raw.owner,
            with_oids=raw.with_oids,
            dependencies=raw.dependencies,
            data_state=raw.data_state,
            offset=raw.offset,
        )
