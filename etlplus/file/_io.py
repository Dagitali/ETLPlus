"""
:mod:`etlplus.file._io` module.

Shared helpers for record normalization and delimited text formats.
"""

from __future__ import annotations

import csv
import warnings
from abc import ABC
from abc import abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar
from typing import Literal
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import StrPath
from ..utils import count_records
from ._sql import resolve_table

if TYPE_CHECKING:
    from .base import ReadOptions
    from .base import WriteOptions

# SECTION: FUNCTIONS ======================================================== #


def coerce_path(
    path: StrPath,
) -> Path:
    """
    Coerce path-like inputs into :class:`~pathlib.Path`.

    Parameters
    ----------
    path : StrPath
        Path-like input to normalize.

    Returns
    -------
    Path
        Normalized :class:`~pathlib.Path` instance.
    """
    return path if isinstance(path, Path) else Path(path)


def coerce_record_payload(
    payload: Any,
    *,
    format_name: str,
) -> JSONData:
    """
    Validate that *payload* is an object or list of objects.

    Parameters
    ----------
    payload : Any
        Parsed payload to validate.
    format_name : str
        Human-readable format name for error messages.

    Returns
    -------
    JSONData
        *payload* when it is a dict or a list of dicts.

    Raises
    ------
    TypeError
        If the payload is not a dict or list of dicts.
    """
    if isinstance(payload, dict):
        return cast(JSONDict, payload)
    if isinstance(payload, list):
        if all(isinstance(item, dict) for item in payload):
            return cast(JSONList, payload)
        raise TypeError(
            f'{format_name} array must contain only objects (dicts)',
        )
    raise TypeError(
        f'{format_name} root must be an object or an array of objects',
    )


def ensure_parent_dir(
    path: StrPath,
) -> None:
    """
    Ensure the parent directory for *path* exists.

    Parameters
    ----------
    path : StrPath
        Target path to ensure the parent directory for.
    """
    path = coerce_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)


def normalize_records(
    data: JSONData,
    format_name: str,
) -> JSONList:
    """
    Normalize payloads into a list of dictionaries.

    Parameters
    ----------
    data : JSONData
        Input payload to normalize.
    format_name : str
        Human-readable format name for error messages.

    Returns
    -------
    JSONList
        Normalized list of dictionaries.

    Raises
    ------
    TypeError
        If the payload is not a dict or a list of dicts.
    """
    if isinstance(data, list):
        if not all(isinstance(item, dict) for item in data):
            raise TypeError(
                f'{format_name} payloads must contain only objects (dicts)',
            )
        return cast(JSONList, data)
    if isinstance(data, dict):
        return [cast(JSONDict, data)]
    raise TypeError(
        f'{format_name} payloads must be an object or an array of objects',
    )


def read_delimited(
    path: StrPath,
    *,
    delimiter: str,
) -> JSONList:
    """
    Read delimited content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the delimited file on disk.
    delimiter : str
        Delimiter character for parsing.

    Returns
    -------
    JSONList
        The list of dictionaries read from the delimited file.
    """
    path = coerce_path(path)
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader: csv.DictReader[str] = csv.DictReader(
            handle,
            delimiter=delimiter,
        )
        rows: JSONList = []
        for row in reader:
            if not any(row.values()):
                continue
            rows.append(cast(JSONDict, dict(row)))
    return rows


def read_text(
    path: StrPath,
    *,
    encoding: str = 'utf-8',
) -> str:
    """
    Read and return text content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the text file on disk.
    encoding : str, optional
        Text encoding. Defaults to ``'utf-8'``.

    Returns
    -------
    str
        File contents as text.
    """
    path = coerce_path(path)
    with path.open('r', encoding=encoding) as handle:
        return handle.read()


def records_from_table(
    table: Any,
) -> JSONList:
    """
    Convert a table/dataframe-like object to row records.

    Parameters
    ----------
    table : Any
        Object exposing ``to_dict(orient='records')``.

    Returns
    -------
    JSONList
        Converted row records.
    """
    return cast(JSONList, table.to_dict(orient='records'))


def require_dict_payload(
    data: JSONData,
    *,
    format_name: str,
) -> JSONDict:
    """
    Validate that *data* is a dictionary payload.

    Parameters
    ----------
    data : JSONData
        Input payload to validate.
    format_name : str
        Human-readable format name for error messages.

    Returns
    -------
    JSONDict
        Validated dictionary payload.

    Raises
    ------
    TypeError
        If the payload is not a dictionary.
    """
    if isinstance(data, list) or not isinstance(data, dict):
        raise TypeError(f'{format_name} payloads must be a dict')
    return cast(JSONDict, data)


def require_str_key(
    payload: JSONDict,
    *,
    format_name: str,
    key: str,
) -> str:
    """
    Require a string value for *key* in *payload*.

    Parameters
    ----------
    payload : JSONDict
        Dictionary payload to inspect.
    format_name : str
        Human-readable format name for error messages.
    key : str
        Key to extract.

    Returns
    -------
    str
        The string value for *key*.

    Raises
    ------
    TypeError
        If the key is missing or not a string.
    """
    value = payload.get(key)
    if not isinstance(value, str):
        raise TypeError(
            f'{format_name} payloads must include a "{key}" string',
        )
    return value


def stringify_value(value: Any) -> str:
    """
    Normalize configuration-like values into strings.

    Parameters
    ----------
    value : Any
        Value to normalize.

    Returns
    -------
    str
        Stringified value (``''`` for ``None``).
    """
    if value is None:
        return ''
    return str(value)


def warn_deprecated_module_io(
    module_name: str,
    operation: Literal['read', 'write'],
) -> None:
    """
    Emit a deprecation warning for module-level IO wrappers.

    Parameters
    ----------
    module_name : str
        Fully-qualified module name containing the deprecated wrapper.
    operation : Literal['read', 'write']
        Deprecated module-level operation.
    """
    warnings.warn(
        (
            f'{module_name}.{operation}() is deprecated; use handler '
            'instance methods instead.'
        ),
        DeprecationWarning,
        stacklevel=3,
    )


def write_delimited(
    path: StrPath,
    data: JSONData,
    *,
    delimiter: str,
    format_name: str = 'Delimited',
) -> int:
    """
    Write *data* to a delimited file and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the delimited file on disk.
    data : JSONData
        Data to write as delimited rows.
    delimiter : str
        Delimiter character for writing.
    format_name : str, optional
        Human-readable format name for error messages. Defaults to
        ``'Delimited'``.

    Returns
    -------
    int
        The number of rows written.
    """
    path = coerce_path(path)
    rows = normalize_records(data, format_name)

    fieldnames = sorted({key for row in rows for key in row})
    ensure_parent_dir(path)
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            delimiter=delimiter,
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})

    return len(rows)


def write_text(
    path: StrPath,
    text: str,
    *,
    encoding: str = 'utf-8',
    trailing_newline: bool = False,
) -> None:
    """
    Write text content to *path*.

    Parameters
    ----------
    path : StrPath
        Path to the text file on disk.
    text : str
        Text content to write.
    encoding : str, optional
        Text encoding. Defaults to ``'utf-8'``.
    trailing_newline : bool, optional
        Whether to append a trailing newline if missing.
    """
    ensure_parent_dir(path)
    payload = text
    if trailing_newline and not payload.endswith('\n'):
        payload = f'{payload}\n'
    path = coerce_path(path)
    with path.open('w', encoding=encoding, newline='') as handle:
        handle.write(payload)


def call_deprecated_module_read[T](
    path: StrPath,
    module_name: str,
    reader: Callable[[Path], T],
) -> T:
    """
    Delegate deprecated module :func:`read` wrappers to handler methods.

    Parameters
    ----------
    path : StrPath
        Path-like wrapper argument to normalize.
    module_name : str
        Fully-qualified module name containing the deprecated wrapper.
    reader : Callable[[Path], T]
        Bound handler read method.

    Returns
    -------
    T
        Parsed payload returned by the handler read method.
    """
    warn_deprecated_module_io(module_name, 'read')
    return reader(coerce_path(path))


def call_deprecated_module_write(
    path: StrPath,
    data: JSONData,
    module_name: str,
    writer: Callable[[Path, JSONData], int],
) -> int:
    """
    Delegate deprecated module :func:`write` wrappers to handler methods.

    Parameters
    ----------
    path : StrPath
        Path-like wrapper argument to normalize.
    data : JSONData
        Payload forwarded to the handler write method.
    module_name : str
        Fully-qualified module name containing the deprecated wrapper.
    writer : Callable[[Path, JSONData], int]
        Bound handler write method.

    Returns
    -------
    int
        Number of records written by the handler.
    """
    warn_deprecated_module_io(module_name, 'write')
    return writer(coerce_path(path), data)


# SECTION: CLASSES ========================================================== #


class FileHandlerOption:
    """
    Shared helpers for common read/write option extraction.
    """

    # -- Internal Instance Methods -- #

    def _option_attr(
        self,
        options: ReadOptions | WriteOptions | None,
        attr_name: str,
    ) -> Any | None:
        """
        Return one option attribute value when present.

        Parameters
        ----------
        options : ReadOptions | WriteOptions | None
            Options object to extract the attribute from.
        attr_name : str
            Name of the attribute to extract.

        Returns
        -------
        Any | None
            The attribute value when present, else ``None``.
        """
        if options is None:
            return None
        return getattr(options, attr_name)

    # -- Instance Methods -- #

    def encoding_from_read_options(
        self,
        options: ReadOptions | None,
        *,
        default: str = 'utf-8',
    ) -> str:
        """
        Extract text encoding from read options.

        Parameters
        ----------
        options : ReadOptions | None
            Read options to extract the encoding from.
        default : str, optional
            Default encoding to return when not specified in *options*.
            Defaults to ``'utf-8'``.

        Returns
        -------
        str
            Text encoding from *options* when present, else *default*.
        """
        value = self._option_attr(options, 'encoding')
        if value is not None:
            return cast(str, value)
        return default

    def encoding_from_write_options(
        self,
        options: WriteOptions | None,
        *,
        default: str = 'utf-8',
    ) -> str:
        """
        Extract text encoding from write options.

        Parameters
        ----------
        options : WriteOptions | None
            Write options to extract the encoding from.
        default : str, optional
            Default encoding to return when not specified in *options*.
            Defaults to ``'utf-8'``.

        Returns
        -------
        str
            Text encoding from *options* when present, else *default*.
        """
        value = self._option_attr(options, 'encoding')
        if value is not None:
            return cast(str, value)
        return default

    def read_extra_option(
        self,
        options: ReadOptions | None,
        key: str,
        *,
        default: Any | None = None,
    ) -> Any | None:
        """
        Read one format-specific read option from ``options.extras``.

        Parameters
        ----------
        options : ReadOptions | None
            Read options to extract the extra option from.
        key : str
            Key of the extra option to extract.
        default : Any | None, optional
            Default value to return when the extra option is not present.
            Defaults to ``None``.

        Returns
        -------
        Any | None
            The value of the extra option when present, else *default*.
        """
        if options is None:
            return default
        return options.extras.get(key, default)

    def root_tag_from_write_options(
        self,
        options: WriteOptions | None,
        *,
        default: str = 'root',
    ) -> str:
        """
        Extract XML-like root tag from write options.

        Parameters
        ----------
        options : WriteOptions | None
            Write options to extract the root tag from.
        default : str, optional
            Default root tag to return when not specified in *options*.
            Defaults to ``'root'``.

        Returns
        -------
        str
            XML-like root tag from *options* when present, else *default*.
        """
        value = self._option_attr(options, 'root_tag')
        if value is not None:
            return cast(str, value)
        return default

    def write_extra_option(
        self,
        options: WriteOptions | None,
        key: str,
        *,
        default: Any | None = None,
    ) -> Any | None:
        """
        Read one format-specific write option from ``options.extras``.

        Parameters
        ----------
        options : WriteOptions | None
            Write options to extract the extra option from.
        key : str
            Key of the extra option to extract.
        default : Any | None, optional
            Default value to return when the extra option is not present.
            Defaults to ``None``.

        Returns
        -------
        Any | None
            The value of the extra option when present, else *default*.
        """
        if options is None:
            return default
        return options.extras.get(key, default)


# SECTION: ABSTRACT BASE CLASSES ============================================ #


class BinarySerializationABC(ABC):
    """
    Shared path-level read/write flow for binary serialization handlers.
    """

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def dumps_bytes(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> bytes:
        """
        Serialize structured data into binary payload bytes.
        """

    @abstractmethod
    def loads_bytes(
        self,
        payload: bytes,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse binary payload bytes into structured data.

        Parameters
        ----------
        payload : bytes
            Binary payload to parse.
        options : ReadOptions | None, optional
            Read options to use when parsing the payload.
            Defaults to ``None``.

        Returns
        -------
        JSONData
            Structured data parsed from the binary payload.
        """

    def count_written_records(
        self,
        data: JSONData,
    ) -> int:
        """
        Return the default record count for binary write operations.
        """
        return count_records(data)

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and decode binary serialization payload bytes from *path*.

        Parameters
        ----------
        path : Path
            Path to read the binary payload from.
        options : ReadOptions | None, optional
            Read options to use when parsing the payload.
            Defaults to ``None``.

        Returns
        -------
        JSONData
            Structured data parsed from the binary payload.
        """
        return self.loads_bytes(path.read_bytes(), options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Encode and write binary serialization payload bytes to *path*.

        Parameters
        ----------
        path : Path
            Path to write the binary payload to.
        data : JSONData
            Structured data to serialize and write.
        options : WriteOptions | None, optional
            Write options to use when encoding the payload.
            Defaults to ``None``.

        Returns
        -------
        int
            Number of records written.
        """
        payload = self.dumps_bytes(data, options=options)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return self.count_written_records(data)


class ColumnarABC(ABC):
    """
    Shared read/write dispatch for columnar table handlers.
    """

    # -- Class Attributes -- #

    format_name: str

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def read_table(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> Any:
        """
        Read a columnar table object from *path*.

        Parameters
        ----------
        path : Path
            Path to read the columnar table from.
        options : ReadOptions | None, optional
            Read options to use when parsing the table.
            Defaults to ``None``.

        Returns
        -------
        Any
            Columnar table object read from *path*.
        """

    @abstractmethod
    def write_table(
        self,
        path: Path,
        table: Any,
        *,
        options: WriteOptions | None = None,
    ) -> None:
        """
        Write a columnar table object to *path*.

        Parameters
        ----------
        path : Path
            Path to write the columnar table to.
        table : Any
            Columnar table object to write.
        options : WriteOptions | None, optional
            Write options to use when encoding the table.
            Defaults to ``None``.
        """

    @abstractmethod
    def table_to_records(
        self,
        table: Any,
    ) -> JSONList:
        """
        Convert a table object into row-oriented records.

        Parameters
        ----------
        table : Any
            Columnar table object to convert.

        Returns
        -------
        JSONList
            Row-oriented records extracted from the table.
        """

    @abstractmethod
    def records_to_table(
        self,
        data: JSONData,
    ) -> Any:
        """
        Convert row-oriented records into a table object.

        Parameters
        ----------
        data : JSONData
            Row-oriented records to convert.

        Returns
        -------
        Any
            Columnar table object created from the records.
        """

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return columnar content from *path*.

        Parameters
        ----------
        path : Path
            Path to read the columnar table from.
        options : ReadOptions | None, optional
            Read options to use when parsing the table.
            Defaults to ``None``.

        Returns
        -------
        JSONList
            Row-oriented records extracted from the columnar table.
        """
        return self.table_to_records(self.read_table(path, options=options))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write columnar content to *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to write the columnar table to.
        data : JSONData
            Row-oriented records to write.
        options : WriteOptions | None, optional
            Write options to use when encoding the table.
            Defaults to ``None``.

        Returns
        -------
        int
            Number of records written.
        """
        rows = normalize_records(data, self.format_name)
        if not rows:
            return 0
        path.parent.mkdir(parents=True, exist_ok=True)
        self.write_table(
            path,
            self.records_to_table(rows),
            options=options,
        )
        return len(rows)


class EmbeddedDatabaseABC(FileHandlerOption, ABC):
    """
    Shared read/write dispatch for embedded-database handlers.
    """

    # -- Class Attributes -- #

    engine_name: ClassVar[str]
    default_table: ClassVar[str]
    format_name: str

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def connect(
        self,
        path: Path,
    ) -> Any:
        """
        Open and return a database connection for *path*.
        """

    @abstractmethod
    def list_tables(
        self,
        connection: Any,
    ) -> list[str]:
        """
        Return readable table names from *connection*.
        """

    @abstractmethod
    def read_table(
        self,
        connection: Any,
        table: str,
    ) -> JSONList:
        """
        Read rows from *table*.
        """

    @abstractmethod
    def write_table(
        self,
        connection: Any,
        table: str,
        rows: JSONList,
    ) -> int:
        """
        Write *rows* to *table*.
        """

    # -- Instance Methods -- #

    def table_from_read_options(
        self,
        options: ReadOptions | None,
        *,
        default: str | None = None,
    ) -> str | None:
        """
        Extract table selector from read options.
        """
        value = self._option_attr(options, 'table')
        if value is not None:
            return cast(str, value)
        return default

    def table_from_write_options(
        self,
        options: WriteOptions | None,
        *,
        default: str | None = None,
    ) -> str | None:
        """
        Extract table selector from write options.
        """
        value = self._option_attr(options, 'table')
        if value is not None:
            return cast(str, value)
        return default

    def close_connection(
        self,
        connection: Any,
    ) -> None:
        """
        Close a database connection.
        """
        closer = getattr(connection, 'close', None)
        if callable(closer):
            closer()

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return embedded-database content from *path*.

        Parameters
        ----------
        path : Path
            Path to read the embedded-database content from.
        options : ReadOptions | None, optional
            Read options to use when parsing the database.
            Defaults to ``None``.

        Returns
        -------
        JSONList
            Row-oriented records extracted from the embedded-database.
        """
        database_handler = cast(Any, self)
        connection = self.connect(path)
        try:
            table = database_handler.table_from_read_options(options)
            if table is None:
                table = resolve_table(
                    self.list_tables(connection),
                    engine_name=self.engine_name,
                    default_table=self.default_table,
                )
                if table is None:
                    return []
            return self.read_table(connection, table)
        finally:
            database_handler.close_connection(connection)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write embedded-database content to *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to write the embedded-database content to.
        data : JSONData
            Row-oriented records to write.
        options : WriteOptions | None, optional
            Write options to use when encoding the database.
            Defaults to ``None``.

        Returns
        -------
        int
            Number of records written.

        Raises
        ------
        ValueError
            If no table name is specified in options and no default table can
            be resolved from the database.
        """
        database_handler = cast(Any, self)
        rows = normalize_records(data, self.format_name)
        if not rows:
            return 0
        table = database_handler.table_from_write_options(
            options,
            default=self.default_table,
        )
        if table is None:  # pragma: no cover - guarded by default
            raise ValueError(f'{self.format_name} write requires a table name')
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = self.connect(path)
        try:
            return self.write_table(connection, table, rows)
        finally:
            database_handler.close_connection(connection)


class RowReadWriteABC(ABC):
    """
    Shared read/write dispatch for row-oriented file handlers.
    """

    # -- Class Attributes -- #

    format_name: str

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read row records from *path*.
        """

    @abstractmethod
    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write row records to *path*.
        """

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return row-oriented content from *path*.
        """
        return self.read_rows(path, options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write row-oriented content to *path* and return record count.
        """
        return self.write_rows(
            path,
            normalize_records(data, self.format_name),
            options=options,
        )


class SemiStructuredTextABC(FileHandlerOption, ABC):
    """
    Shared path-level read/write flow for semi-structured text handlers.
    """

    # -- Class Attributes -- #

    write_trailing_newline: ClassVar[bool] = False

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize structured data into text.
        """

    @abstractmethod
    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse *text* into structured data.
        """

    # -- Instance Methods -- #

    def count_written_records(
        self,
        data: JSONData,
    ) -> int:
        """
        Return the default record count for write operations.
        """
        return count_records(data)

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return semi-structured text content from *path*.
        """
        return self.loads(
            read_text(path, encoding=self.encoding_from_read_options(options)),
            options=options,
        )

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write semi-structured text content to *path* and return record count.
        """
        write_text(
            path,
            self.dumps(data, options=options),
            encoding=self.encoding_from_write_options(options),
            trailing_newline=self.write_trailing_newline,
        )
        return self.count_written_records(data)


class ScientificDatasetOptionABC(FileHandlerOption):
    """
    Shared helpers for scientific dataset selection options.
    """

    # -- Instance Methods -- #

    def dataset_from_read_options(
        self,
        options: ReadOptions | None,
    ) -> str | None:
        """
        Extract dataset selector from read options.
        """
        value = self._option_attr(options, 'dataset')
        if value is not None:
            return cast(str, value)
        return None

    def dataset_from_write_options(
        self,
        options: WriteOptions | None,
    ) -> str | None:
        """
        Extract dataset selector from write options.
        """
        value = self._option_attr(options, 'dataset')
        if value is not None:
            return cast(str, value)
        return None

    def resolve_read_dataset(
        self,
        dataset: str | None = None,
        *,
        options: ReadOptions | None = None,
        default: str | None = None,
    ) -> str | None:
        """
        Resolve read-time dataset selection using explicit, options, then
        default.
        """
        if dataset is not None:
            return dataset
        from_options = self.dataset_from_read_options(options)
        if from_options is not None:
            return from_options
        return default

    def resolve_write_dataset(
        self,
        dataset: str | None = None,
        *,
        options: WriteOptions | None = None,
        default: str | None = None,
    ) -> str | None:
        """
        Resolve write-time dataset selection using explicit, options, then
        default.
        """
        if dataset is not None:
            return dataset
        from_options = self.dataset_from_write_options(options)
        if from_options is not None:
            return from_options
        return default


class SpreadsheetSheetOptionABC(FileHandlerOption):
    """
    Shared helpers for spreadsheet sheet-selection options.
    """

    # -- Class Attributes -- #

    default_sheet: ClassVar[str | int]

    # -- Instance Methods -- #

    def sheet_from_read_options(
        self,
        options: ReadOptions | None,
        *,
        default: str | int | None = None,
    ) -> str | int:
        """
        Extract sheet selector from read options.
        """
        value = self._option_attr(options, 'sheet')
        if value is not None:
            return cast(str | int, value)
        if default is not None:
            return default
        return self.default_sheet

    def sheet_from_write_options(
        self,
        options: WriteOptions | None,
        *,
        default: str | int | None = None,
    ) -> str | int:
        """
        Extract sheet selector from write options.
        """
        value = self._option_attr(options, 'sheet')
        if value is not None:
            return cast(str | int, value)
        if default is not None:
            return default
        return self.default_sheet


class ScientificDataseABC(ScientificDatasetOptionABC, ABC):
    """
    Shared read/write dispatch for scientific dataset handlers.
    """

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return one dataset from *path*.

        Parameters
        ----------
        path : Path
            The path to the file containing the dataset.
        dataset : str | None, optional
            The name of the dataset to read. If not provided, the default
            dataset will be used.
        options : ReadOptions | None, optional
            Additional options for reading the dataset.

        Returns
        -------
        JSONData
            The content of the dataset.

        Raises
        ------
        ValueError
            If the specified dataset does not exist.
        """

    @abstractmethod
    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write one dataset to *path* and return record count.

        Parameters
        ----------
        path : Path
            The path to the file where the dataset will be written.
        data : JSONData
            The data to write to the dataset.
        dataset : str | None, optional
            The name of the dataset to write. If not provided, the default
            dataset will be used.
        options : WriteOptions | None, optional
            Additional options for writing the dataset.

        Returns
        -------
        int
            The number of records written to the dataset.
        """

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return scientific dataset content from *path*.

        Parameters
        ----------
        path : Path
            The path to the file containing the dataset.
        options : ReadOptions | None, optional
            Additional options for reading the dataset.

        Returns
        -------
        JSONData
            The content of the dataset.
        """
        dataset = self.dataset_from_read_options(options)
        return self.read_dataset(path, dataset=dataset, options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write scientific dataset content to *path* and return record count.

        Parameters
        ----------
        path : Path
            The path to the file where the dataset will be written.
        data : JSONData
            The data to write to the dataset.
        options : WriteOptions | None, optional
            Additional options for writing the dataset.

        Returns
        -------
        int
            The number of records written to the dataset.
        """
        dataset = self.dataset_from_write_options(options)
        return self.write_dataset(
            path,
            data,
            dataset=dataset,
            options=options,
        )


class SpreadsheetSheetABC(SpreadsheetSheetOptionABC, ABC):
    """
    Shared read/write dispatch for spreadsheet handlers.
    """

    # -- Class Attributes -- #

    format_name: str

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def read_sheet(
        self,
        path: Path,
        *,
        sheet: str | int,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read a single sheet from *path*.
        """

    @abstractmethod
    def write_sheet(
        self,
        path: Path,
        rows: JSONList,
        *,
        sheet: str | int,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write rows to a single sheet in *path*.
        """

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return spreadsheet content from *path*.
        """
        sheet = self.sheet_from_read_options(options)
        return self.read_sheet(path, sheet=sheet, options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write spreadsheet content to *path* and return record count.

        Parameters
        ----------
        path : Path
            The path to the file where the spreadsheet will be written.
        data : JSONData
            The data to write to the spreadsheet.
        options : WriteOptions | None, optional
            Additional options for writing the spreadsheet.

        Returns
        -------
        int
            The number of records written to the spreadsheet.
        """
        rows = normalize_records(data, self.format_name)
        if not rows:
            return 0
        sheet = self.sheet_from_write_options(options)
        path.parent.mkdir(parents=True, exist_ok=True)
        return self.write_sheet(path, rows, sheet=sheet, options=options)
