"""
:mod:`etlplus.ops.load` module.

Helpers to load data into files, databases, and REST APIs.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from ..api import HttpMethod
from ..api import compose_api_target_env
from ..api._utils import ApiTargetEnvDict
from ..connector import DataConnectorType
from ..file import File
from ..file import FileFormat
from ..file._core import FileFormatArg
from ..file.base import WriteOptions
from ..storage import StorageLocation
from ..utils import count_records
from ..utils._types import JSONData
from ..utils._types import JSONDict
from ..utils._types import StrPath
from ._database import DATABASE_DRIVER_NOTE
from ._database import DATABASE_LOAD_NOT_IMPLEMENTED
from ._files import resolve_file
from ._http import DirectRequestEnvDict
from ._http import build_direct_request_env
from ._http import build_request_call
from ._http import response_json_or_text
from ._http import send_request
from ._options import coerce_write_options as _coerce_write_options
from ._types import ConnectorTypeArg
from ._types import DataSourceArg
from ._types import FileOptionsArg

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'load',
    'load_data',
    'load_to_api',
    'load_to_database',
    'load_to_file',
]


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
    env: ApiTargetEnvDict | DirectRequestEnvDict,
) -> JSONDict:
    """
    Load data to an API target using a normalized environment.

    Parameters
    ----------
    data : JSONData
        Payload to load.
    env : ApiTargetEnvDict | DirectRequestEnvDict
        Normalized request environment.

    Returns
    -------
    JSONDict
        Load result payload.
    """
    request = build_request_call(
        env,
        error_message='API target missing "url"',
        default_method=HttpMethod.POST,
        json_data=data,
    )
    response = send_request(request)

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
    source: DataSourceArg,
) -> JSONData:
    """
    Load data from a file path, JSON string, or direct object.

    Parameters
    ----------
    source : DataSourceArg
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
    env = build_direct_request_env(url, method, kwargs)
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
        'message': DATABASE_LOAD_NOT_IMPLEMENTED,
        'connection_string': connection_string,
        'records': records,
        'note': DATABASE_DRIVER_NOTE,
    }


def load_to_file(
    data: JSONData,
    file_path: StrPath,
    file_format: FileFormatArg = None,
    options: FileOptionsArg[WriteOptions] = None,
) -> JSONDict:
    """
    Persist data to a local file path or remote URI.

    Parameters
    ----------
    data : JSONData
        Data to write.
    file_path : StrPath
        Target local file path or remote URI.
    file_format : FileFormatArg, optional
        Output format. If omitted (None), the format is inferred from the
        filename extension.
    options : FileOptionsArg[WriteOptions], optional
        Optional file-write options such as ``encoding`` plus format-specific
        extras like ``delimiter``.

    Returns
    -------
    JSONDict
        Result dictionary with status and record count.
    """
    resolved_options = _coerce_write_options(options)
    target_label = str(file_path)
    target = resolve_file(
        file_path,
        file_format,
        inferred_default=FileFormat.JSON,
        file_cls=File,
    )
    records = (
        target.file.write(data)
        if resolved_options is None
        else target.file.write(data, options=resolved_options)
    )
    message = (
        'No data to write'
        if target.file_format is FileFormat.CSV and records == 0
        else f'Data loaded to {target_label}'
    )

    return {
        'status': 'success',
        'message': message,
        'records': records,
    }


# -- Orchestration -- #


def load(
    source: DataSourceArg,
    target_type: ConnectorTypeArg,
    target: StrPath,
    file_format: FileFormatArg = None,
    method: HttpMethod | str | None = None,
    **kwargs: Any,
) -> JSONData:
    """
    Load data to a target (file, database, or API).

    Parameters
    ----------
    source : DataSourceArg
        Data source to load.
    target_type : ConnectorTypeArg
        Type of data target.
    target : StrPath
        Target location (file path, connection string, or API URL).
    file_format : FileFormatArg, optional
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
