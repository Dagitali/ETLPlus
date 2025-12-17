"""
:mod:`etlplus.load` module.

Helpers to load data into files, databases, and REST APIs.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any
from typing import cast

import requests  # type: ignore[import]

from .enums import DataConnectorType
from .enums import FileFormat
from .enums import HttpMethod
from .enums import coerce_data_connector_type
from .enums import coerce_file_format
from .enums import coerce_http_method
from .file import File
from .types import JSONData
from .types import JSONDict
from .types import JSONList
from .types import StrPath
from .utils import count_records

# SECTION: INTERNAL FUNCTIONS ============================================== #


def _parse_json_string(
    raw: str,
) -> JSONData:
    """
    Parse JSON data from ``raw`` text.

    Parameters
    ----------
    raw : str
        Raw JSON string to parse.

    Returns
    -------
    JSONData
        Parsed object or list of objects.

    Raises
    ------
    ValueError
        If the JSON is invalid or not an object/array.
    """
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f'Invalid data source: {raw}') from exc

    if isinstance(loaded, dict):
        return cast(JSONDict, loaded)
    if isinstance(loaded, list):
        if all(isinstance(item, dict) for item in loaded):
            return cast(JSONList, loaded)
        raise ValueError(
            'JSON array must contain only objects (dicts) when parsing string',
        )
    raise ValueError(
        'JSON root must be an object or array when parsing string',
    )


# SECTION: FUNCTIONS ======================================================== #


# -- Data Loading -- #


def load_data(
    source: StrPath | JSONData,
) -> JSONData:
    """
    Load data from a file path, JSON string, or direct object.

    Parameters
    ----------
    source : StrPath | JSONData
        Data source to load. If a path is provided and exists, JSON will be
        read from it. Otherwise, a JSON string will be parsed.

    Returns
    -------
    JSONData
        Parsed object or list of objects.

    Raises
    ------
    TypeError
        If `source` is not a string, path, or JSON-like object.
    """
    if isinstance(source, (dict, list)):
        return cast(JSONData, source)

    if isinstance(source, Path):
        return File(source, FileFormat.JSON).read_json()

    if isinstance(source, str):
        # Special case: '-' means read JSON from stdin (Unix convention).
        if source == '-':
            raw = sys.stdin.read()
            return _parse_json_string(raw)
        candidate = Path(source)
        if candidate.exists():
            try:
                return File(candidate, FileFormat.JSON).read_json()
            except (OSError, json.JSONDecodeError, ValueError):
                # Fall back to treating the string as raw JSON content.
                pass
        return _parse_json_string(source)

    raise TypeError(
        'source must be a mapping, sequence of mappings, path, or JSON string',
    )


# -- File Loading -- #


def load_to_file(
    data: JSONData,
    file_path: StrPath,
    file_format: FileFormat | str | None = None,
) -> JSONDict:
    """
    Persist data to a local file.

    Parameters
    ----------
    data : JSONData
        Data to write.
    file_path : StrPath
        Target file path.
    file_format : FileFormat | str | None, optional
        Output format. If omitted (None), the format is inferred from the
        filename extension.

    Returns
    -------
    JSONDict
        Result dictionary with status and record count.
    """
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # If no explicit format is provided, let File infer from extension.
    if file_format is None:
        records = File(path).write(data)
        ext = path.suffix.lstrip('.').lower()
        fmt = coerce_file_format(ext) if ext else FileFormat.JSON
    else:
        fmt = coerce_file_format(file_format)
        records = File(path, fmt).write(data)
    if fmt is FileFormat.CSV and records == 0:
        message = 'No data to write'
    else:
        message = f'Data loaded to {path}'

    return {
        'status': 'success',
        'message': message,
        'records': records,
    }


# -- Database Loading (Placeholder) -- #


def load_to_database(
    data: JSONData,
    connection_string: str,
) -> JSONDict:
    """
    Load data to a database.

    Notes
    -----
    Placeholder implementation. To enable database loading, install and
    configure database-specific drivers and query logic.

    Parameters
    ----------
    data : JSONData
        Data to load.
    connection_string : str
        Database connection string.

    Returns
    -------
    JSONDict
        Result object describing the operation.
    """
    records = count_records(data)

    return {
        'status': 'not_implemented',
        'message': 'Database loading not yet implemented',
        'connection_string': connection_string,
        'records': records,
        'note': 'Install database-specific drivers to enable this feature',
    }


# -- REST API Loading -- #


def load_to_api(
    data: JSONData,
    url: str,
    method: HttpMethod | str,
    **kwargs: Any,
) -> JSONDict:
    """
    Load data to a REST API.

    Parameters
    ----------
    data : JSONData
        Data to send as JSON.
    url : str
        API endpoint URL.
    method : HttpMethod | str
        HTTP method to use.
    **kwargs : Any
        Extra arguments forwarded to ``requests`` (e.g., ``timeout``).

    Returns
    -------
    JSONDict
        Result dictionary including response payload or text.

    Raises
    ------
    TypeError
        If the session object is not valid.
    """
    http_method = coerce_http_method(method)

    # Apply a conservative timeout to guard against hanging requests.
    timeout = kwargs.pop('timeout', 10.0)
    session = kwargs.pop('session', None)
    requester = session or requests

    request_callable = getattr(requester, http_method.value, None)
    if not callable(request_callable):
        raise TypeError(
            'Session object must supply a '
            f'callable "{http_method.value}" method',
        )

    response = request_callable(url, json=data, timeout=timeout, **kwargs)
    response.raise_for_status()

    # Try JSON first, fall back to text.
    try:
        payload: Any = response.json()
    except ValueError:
        payload = response.text

    return {
        'status': 'success',
        'status_code': response.status_code,
        'message': f'Data loaded to {url}',
        'response': payload,
        'records': count_records(data),
        'method': http_method.value.upper(),
    }


# -- Orchestration -- #


def load(
    source: StrPath | JSONData,
    target_type: DataConnectorType | str,
    target: StrPath,
    file_format: FileFormat | str | None = None,
    method: HttpMethod | str | None = None,
    **kwargs: Any,
) -> JSONData:
    """
    Load data to a target (file, database, or API).

    Parameters
    ----------
    source : StrPath | JSONData
        Data source to load.
    target_type : DataConnectorType | str
        Type of data target.
    target : StrPath
        Target location (file path, connection string, or API URL).
    file_format : FileFormat | str | None, optional
        File format, inferred from filename extension if omitted.
    method : HttpMethod | str | None, optional
        HTTP method for API targets. Defaults to POST if omitted.
    **kwargs : Any
        Additional arguments forwarded to target-specific loaders.

    Returns
    -------
    JSONData
        Result dictionary with status.

    Raises
    ------
    ValueError
        If `target_type` is not one of the supported values.
    """
    data = load_data(source)

    match coerce_data_connector_type(target_type):
        case DataConnectorType.FILE:
            # Prefer explicit format if provided, else infer from filename.
            return load_to_file(data, target, file_format)
        case DataConnectorType.DATABASE:
            return load_to_database(data, str(target))
        case DataConnectorType.API:
            api_method = method if method is not None else HttpMethod.POST
            return load_to_api(
                data,
                str(target),
                method=api_method,
                **kwargs,
            )
        case _:
            # `coerce_data_connector_type` covers invalid entries, but keep
            # explicit guard.
            raise ValueError(f'Invalid target type: {target_type}')
