"""
:mod:`etlplus.ops.load` module.

Helpers to load data into files, databases, and REST APIs.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..api import HttpMethod
from ..api import compose_api_target_env
from ..connector import DataConnectorType
from ..file import File
from ..file import FileFormat
from ..file.base import WriteOptions
from ..storage import StorageLocation
from ..utils import count_records
from ..utils._types import JSONData
from ..utils._types import JSONDict
from ..utils._types import StrPath
from ._http import build_request_call
from ._http import response_json_or_text
from ._options import coerce_write_options as _coerce_write_options

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'load',
    'load_data',
    'load_to_api',
    'load_to_database',
    'load_to_file',
]


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class _FileWriteTarget:
    """Resolved file target details for one write operation."""

    # -- Instance Attributes -- #

    file: File
    file_format: FileFormat


# SECTION: INTERNAL FUNCTIONS =============================================== #


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

    location = StorageLocation.from_value(source)
    if location.is_local:
        candidate = location.as_path()
        file = File(candidate, FileFormat.JSON)
        exists = candidate.exists()
    else:
        file = File(source, FileFormat.JSON)
        exists = file.exists()

    if exists:
        try:
            return file.read()
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
    request = build_request_call(
        env,
        error_message='API target missing "url"',
        default_method=HttpMethod.POST,
        json_data=data,
    )
    response = request.request_callable(
        request.url,
        timeout=request.timeout,
        **request.kwargs,
    )
    response.raise_for_status()

    return {
        'status': 'success',
        'status_code': response.status_code,
        'message': f'Data loaded to {request.url}',
        'response': response_json_or_text(response),
        'records': count_records(data),
        'method': request.http_method.value.upper(),
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

    match loaded:
        case dict():
            return loaded
        case list():
            if all(isinstance(item, dict) for item in loaded):
                return loaded
            raise ValueError(
                'JSON array must contain only objects (dicts) when parsing string',
            )
        case _:
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
    match source:
        case dict() | list():
            return source
        case Path():
            return File(source, FileFormat.JSON).read()
        case str():
            return _load_data_from_str(source)
        case _:
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
    options: WriteOptions | Mapping[str, Any] | None = None,
) -> JSONDict:
    """
    Persist data to a local file path or remote URI.

    Parameters
    ----------
    data : JSONData
        Data to write.
    file_path : StrPath
        Target local file path or remote URI.
    file_format : FileFormat | str | None, optional
        Output format. If omitted (None), the format is inferred from the
        filename extension.
    options : WriteOptions | Mapping[str, Any] | None, optional
        Optional file-write options such as ``encoding`` plus format-specific
        extras like ``delimiter``.

    Returns
    -------
    JSONDict
        Result dictionary with status and record count.
    """
    resolved_options = _coerce_write_options(options)
    target_label = str(file_path)
    target = _resolve_file_write_target(file_path, file_format)
    records = (
        target.file.write(data)
        if resolved_options is None
        else target.file.write(data, options=resolved_options)
    )

    if target.file_format is FileFormat.CSV and records == 0:
        message = 'No data to write'
    else:
        message = f'Data loaded to {target_label}'

    return {
        'status': 'success',
        'message': message,
        'records': records,
    }


def _resolve_file_write_target(
    file_path: StrPath,
    file_format: FileFormat | str | None,
) -> _FileWriteTarget:
    """Return one file target and its effective format."""
    if file_format is None:
        file = File(file_path)
        return _FileWriteTarget(
            file=file,
            file_format=file.file_format or FileFormat.JSON,
        )

    resolved_format = FileFormat.coerce(file_format)
    return _FileWriteTarget(
        file=File(file_path, resolved_format),
        file_format=resolved_format,
    )


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
            return load_to_file(data, target, file_format, kwargs or None)
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
