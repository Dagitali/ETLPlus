"""
ETLPlus Data Loading
====================

Helpers to load data into files, databases, and REST APIs.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from typing import cast

import requests

from .enums import coerce_data_connector_type
from .enums import coerce_file_format
from .enums import coerce_http_method
from .enums import DataConnectorType
from .enums import FileFormat
from .enums import HttpMethod
from .file import read_json
from .file import write_structured_file
from .types import JSONData
from .types import JSONDict
from .types import JSONList
from .types import StrPath


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _parse_json_string(
    raw: str,
) -> JSONData:
    """
    Parse JSON data from ``raw`` text.
    """

    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid data source: {raw}") from exc

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
    ValueError
        If the input cannot be interpreted as a JSON object or array.
    """

    if isinstance(source, (dict, list)):
        return cast(JSONData, source)

    if isinstance(source, Path):
        return read_json(source)

    if isinstance(source, str):
        candidate = Path(source)
        if candidate.exists():
            try:
                return read_json(candidate)
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
    file_format: FileFormat | str = FileFormat.JSON,
) -> JSONDict:
    """
    Persist data to a local file.

    Parameters
    ----------
    data : JSONData
        Data to write.
    file_path : StrPath
        Target file path.
    file_format : {'json', 'csv', 'xml'}, optional
        Output format. Default is 'json'.

    Returns
    -------
    JSONDict
        Result dictionary with status and record count.

    Raises
    ------
    ValueError
        If `file_format` is not one of the supported formats.
    """

    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fmt = coerce_file_format(file_format)
    records = write_structured_file(path, data, fmt)
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
) -> JSONData:
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

    records = len(data) if isinstance(data, list) else 1
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
    method: str,
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
    method : {'POST', 'PUT', 'PATCH'}
        HTTP method to use.
    **kwargs : Any
        Extra arguments forwarded to ``requests`` (e.g., ``timeout``).

    Returns
    -------
    JSONDict
        Result dictionary including response payload or text.

    Raises
    ------
    requests.RequestException
        If the HTTP request fails or returns an error (i.e., non-2xx) status.
    ValueError
        If ``method`` is not supported.
    """

    http_method = coerce_http_method(method)

    # Apply a conservative timeout to guard against hanging requests.
    timeout = kwargs.pop('timeout', 10.0)
    session = kwargs.pop('session', None)
    requester = session or requests

    request_callable = getattr(requester, http_method.value, None)
    if not callable(request_callable):
        raise TypeError(
            'Session object must supply a callable '
            f'"{http_method.value}" method',
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
        'records': len(data) if isinstance(data, list) else 1,
        'method': http_method.value.upper(),
    }


# -- Orchestration -- #


def load(
    source: StrPath | JSONData,
    target_type: DataConnectorType | str,
    target: StrPath,
    **kwargs: Any,
) -> JSONData:
    """
    Load data to a target.

    Parameters
    ----------
    source : StrPath | JSONData
        Data source to load.
    target_type : DataConnectorType | str
        Type of target to load to.
    target : StrPath
        Target location (file path, connection string, or API URL).
    **kwargs : Any
        Additional arguments (e.g., `format` for files, `method` for APIs).

    Returns
    -------
    JSONData
        Result dictionary with status.

    Raises
    ------
    ValueError
        If `target_type` or options are invalid.
    """

    data = load_data(source)
    ttype = coerce_data_connector_type(target_type)

    if ttype is DataConnectorType.FILE:
        file_format = kwargs.pop(
            'format', kwargs.pop('file_format', FileFormat.JSON),
        )
        return load_to_file(data, target, file_format)

    if ttype is DataConnectorType.DATABASE:
        return load_to_database(data, str(target))

    if ttype is DataConnectorType.API:
        method = kwargs.pop('method', HttpMethod.POST)
        return load_to_api(data, str(target), method, **kwargs)

    # `coerce_data_connector_type` covers invalid entries, but keep explicit
    # guard.
    raise ValueError(f'Invalid target type: {target_type}')
