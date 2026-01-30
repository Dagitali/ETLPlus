"""
:mod:`etlplus.ops.load` module.

Helpers to load data into files, databases, and REST APIs.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import cast

from ..api import HttpMethod
from ..api import compose_api_target_env
from ..api.utils import resolve_request
from ..connector import DataConnectorType
from ..file import File
from ..file import FileFormat
from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import StrPath
from ..utils import count_records

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'load',
    'load_data',
    'load_to_api',
    'load_to_database',
    'load_to_file',
]


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _load_data_from_str(
    source: str,
) -> JSONData:
    """
    Load JSON data from a string or file path.

    Parameters
    ----------
    source : str
        Input string representing a file path or JSON payload.

    Returns
    -------
    JSONData
        Parsed JSON payload.
    """
    # Special case: '-' means read JSON from STDIN (Unix convention).
    if source == '-':
        raw = sys.stdin.read()
        return _parse_json_string(raw)

    candidate = Path(source)
    if candidate.exists():
        try:
            return File(candidate, FileFormat.JSON).read()
        except (OSError, json.JSONDecodeError, ValueError):
            # Fall back to treating the string as raw JSON content.
            pass
    return _parse_json_string(source)


def _load_to_api_env(
    data: JSONData,
    env: Mapping[str, Any],
) -> JSONDict:
    """
    Load data to an API target using a normalized environment.

    Parameters
    ----------
    data : JSONData
        Payload to load.
    env : Mapping[str, Any]
        Normalized request environment.

    Returns
    -------
    JSONDict
        Load result payload.

    Raises
    ------
    ValueError
        If required parameters are missing.
    """
    url = env.get('url')
    if not url:
        raise ValueError('API target missing "url"')
    method = env.get('method') or 'post'
    kwargs: dict[str, Any] = {}
    headers = env.get('headers')
    if headers:
        kwargs['headers'] = cast(dict[str, str], headers)
    if env.get('timeout') is not None:
        kwargs['timeout'] = env.get('timeout')
    session = env.get('session')
    if session is not None:
        kwargs['session'] = session
    extra_kwargs = env.get('request_kwargs')
    if isinstance(extra_kwargs, Mapping):
        kwargs.update(extra_kwargs)
    timeout = kwargs.pop('timeout', 10.0)
    session = kwargs.pop('session', None)
    request_callable, timeout, http_method = resolve_request(
        method,
        session=session,
        timeout=timeout,
    )
    response = request_callable(
        cast(str, url),
        json=data,
        timeout=timeout,
        **kwargs,
    )
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


def _parse_json_string(
    raw: str,
) -> JSONData:
    """
    Parse JSON data from *raw* text.

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


# -- Helpers -- #


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
        return File(source, FileFormat.JSON).read()

    if isinstance(source, str):
        return _load_data_from_str(source)

    raise TypeError(
        'source must be a mapping, sequence of mappings, path, or JSON string',
    )


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
        When omitted, ``timeout`` defaults to 10 seconds.

    Returns
    -------
    JSONDict
        Result dictionary including response payload or text.
    """
    # Apply a conservative timeout to guard against hanging requests.
    env = {
        'url': url,
        'method': method,
        'timeout': kwargs.pop('timeout', 10.0),
        'session': kwargs.pop('session', None),
        'request_kwargs': kwargs,
    }
    return _load_to_api_env(data, env)


def load_to_api_target(
    cfg: Any,
    target_obj: Any,
    overrides: dict[str, Any],
    data: JSONData,
) -> JSONDict:
    """
    Load data to an API target connector.

    Parameters
    ----------
    cfg : Any
        Pipeline configuration.
    target_obj : Any
        Connector configuration.
    overrides : dict[str, Any]
        Load-time overrides.
    data : JSONData
        Payload to load.

    Returns
    -------
    JSONDict
        Load result.
    """
    env = compose_api_target_env(cfg, target_obj, overrides)
    return _load_to_api_env(data, env)


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
        fmt = FileFormat.coerce(ext) if ext else FileFormat.JSON
    else:
        fmt = FileFormat.coerce(file_format)
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

    match DataConnectorType.coerce(target_type):
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
            # :meth:`coerce` already raises for invalid connector types, but
            # keep explicit guard for defensive programming.
            raise ValueError(f'Invalid target type: {target_type}')
