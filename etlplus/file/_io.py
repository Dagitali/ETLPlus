"""
:mod:`etlplus.file._io` module.

Shared helpers for record normalization and delimited text formats.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar
from typing import TypeGuard
from typing import cast

from ..utils.types import JSONData
from ..utils.types import JSONDict
from ..utils.types import JSONList
from ..utils.types import StrPath

if TYPE_CHECKING:
    from .base import ReadOptions
    from .base import WriteOptions

# SECTION: INTERNAL FUNCTIONS =============================================== #


def _is_object_list(
    payload: object,
) -> TypeGuard[JSONList]:
    """
    Return whether *payload* is a list containing only dictionary objects.
    """
    return isinstance(payload, list) and all(
        isinstance(item, dict) for item in payload
    )


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
    if _is_object_list(payload):
        return payload
    if isinstance(payload, list):
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
    if _is_object_list(data):
        return data
    if isinstance(data, list):
        raise TypeError(
            f'{format_name} payloads must contain only objects (dicts)',
        )
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


def read_sas_table(
    pandas: Any,
    path: StrPath,
    *,
    format_hint: str | None = None,
) -> Any:
    """
    Read a SAS-backed table via pandas, tolerating unsupported format kwargs.

    Some pandas-compatible readers accept ``format=...`` while others raise
    ``TypeError``; this helper preserves a single fallback path.
    """
    resolved_path = coerce_path(path)
    if format_hint is None:
        return pandas.read_sas(resolved_path)
    try:
        return pandas.read_sas(resolved_path, format=format_hint)
    except TypeError:
        return pandas.read_sas(resolved_path)


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
    if isinstance(data, dict):
        return cast(JSONDict, data)
    raise TypeError(f'{format_name} payloads must be a dict')


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


# SECTION: CLASSES ========================================================== #


class FileHandlerOption:
    """
    Shared helpers for common read/write option extraction.
    """

    # -- Internal Instance Methods -- #

    def _option_attr[T](
        self,
        options: ReadOptions | WriteOptions | None,
        attr_name: str,
        *,
        default: T | None = None,
    ) -> T | None:
        """
        Return one option attribute value or a provided default.

        Parameters
        ----------
        options : ReadOptions | WriteOptions | None
            Options object to extract the attribute from.
        attr_name : str
            Name of the attribute to extract.
        default : T | None, optional
            Fallback value to return when the attribute is missing.
            Defaults to ``None``.

        Returns
        -------
        T | None
            The attribute value when present, else *default*.
        """
        if options is None:
            return default
        value = getattr(options, attr_name)
        return default if value is None else cast(T, value)

    def _resolve_option[T](
        self,
        explicit: T | None,
        options: ReadOptions | WriteOptions | None,
        attr_name: str,
        *,
        default: T | None = None,
    ) -> T | None:
        """
        Resolve one value using explicit, options, then default precedence.
        """
        if explicit is not None:
            return explicit
        return self._option_attr(
            options,
            attr_name,
            default=default,
        )

    # -- Instance Methods -- #

    def encoding_from_options(
        self,
        options: ReadOptions | WriteOptions | None,
        *,
        default: str = 'utf-8',
    ) -> str:
        """
        Extract text encoding from read/write options.

        Parameters
        ----------
        options : ReadOptions | WriteOptions | None
            Read or write options to extract the encoding from.
        default : str, optional
            Default encoding to return when not specified in *options*.
            Defaults to ``'utf-8'``.

        Returns
        -------
        str
            Text encoding from *options* when present, else *default*.
        """
        encoding = self._option_attr(
            options,
            'encoding',
            default=default,
        )
        assert encoding is not None
        return encoding

    def extra_option(
        self,
        options: ReadOptions | WriteOptions | None,
        key: str,
        *,
        default: Any | None = None,
    ) -> Any | None:
        """
        Read one format-specific option from ``options.extras``.

        Parameters
        ----------
        options : ReadOptions | WriteOptions | None
            Read or write options to extract the extra option from.
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
        root_tag = self._option_attr(
            options,
            'root_tag',
            default=default,
        )
        assert root_tag is not None
        return root_tag


class ArchiveInnerNameOption(FileHandlerOption):
    """
    Shared helpers for archive member selection options.
    """

    # -- Instance Methods -- #

    def inner_name_from_options(
        self,
        options: ReadOptions | WriteOptions | None,
        *,
        default: str | None = None,
    ) -> str | None:
        """
        Extract archive member selector from read/write options.
        """
        return self._option_attr(options, 'inner_name', default=default)


class DelimitedOption(FileHandlerOption):
    """
    Shared helpers for delimiter overrides on delimited text handlers.
    """

    # -- Class Attributes -- #

    delimiter: ClassVar[str]

    # -- Instance Methods -- #

    def delimiter_from_options(
        self,
        options: ReadOptions | WriteOptions | None,
        *,
        default: str | None = None,
    ) -> str:
        """
        Extract delimiter override from read/write options.
        """
        override = self.extra_option(options, 'delimiter')
        if override is not None:
            return str(override)
        if default is not None:
            return default
        return self.delimiter


class EmbeddedDatabaseTableOption(FileHandlerOption):
    """
    Shared helpers for embedded-database table selection and cleanup.
    """

    # -- Instance Methods -- #

    def close_connection(
        self,
        connection: Any,
    ) -> None:
        """
        Close a database connection when it exposes a ``close`` method.
        """
        closer = getattr(connection, 'close', None)
        if callable(closer):
            closer()

    def table_from_options(
        self,
        options: ReadOptions | WriteOptions | None,
        *,
        default: str | None = None,
    ) -> str | None:
        """
        Extract table selector from read/write options.
        """
        return self._option_attr(options, 'table', default=default)


class ScientificDatasetOption(FileHandlerOption):
    """
    Shared helpers for scientific dataset selection options.
    """

    # -- Instance Methods -- #

    def dataset_from_options(
        self,
        options: ReadOptions | WriteOptions | None,
    ) -> str | None:
        """
        Extract dataset selector from read/write options.
        """
        return self._option_attr(options, 'dataset', default=None)

    def resolve_dataset(
        self,
        dataset: str | None = None,
        *,
        options: ReadOptions | WriteOptions | None = None,
        default: str | None = None,
    ) -> str | None:
        """
        Resolve dataset selection using explicit, options, then
        default.
        """
        return self._resolve_option(
            dataset,
            options,
            'dataset',
            default=default,
        )


class SpreadsheetSheetOption(FileHandlerOption):
    """
    Shared helpers for spreadsheet sheet-selection options.
    """

    default_sheet: ClassVar[str | int]

    def sheet_from_options(
        self,
        options: ReadOptions | WriteOptions | None,
        *,
        default: str | int | None = None,
    ) -> str | int:
        """
        Extract sheet selector from read/write options.
        """
        resolved_default = self.default_sheet if default is None else default
        sheet = self._option_attr(
            options,
            'sheet',
            default=resolved_default,
        )
        assert sheet is not None
        return cast(str | int, sheet)
