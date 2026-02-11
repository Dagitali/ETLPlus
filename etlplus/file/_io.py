"""
:mod:`etlplus.file._io` module.

Shared helpers for record normalization and delimited text formats.
"""

from __future__ import annotations

import csv
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any
from typing import Literal
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import StrPath

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
