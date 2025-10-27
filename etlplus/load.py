"""
ETLPlus Data Loading
====================

Helpers to load data into files, databases, and REST APIs.
"""
from __future__ import annotations

import csv
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from typing import cast

import requests

from .enums import DataConnectorType
from .enums import FileFormat
from .enums import HttpMethod
from .types import JSONData
from .types import JSONDict
from .types import JSONList
from .types import StrPath


# SECTION: PROTECTED FUNCTIONS ============================================== #


# -- File Loading -- #


def _dict_to_element(
    name: str,
    payload: Any,
) -> ET.Element:
    """
    Convert JSON-like structures into XML elements.
    """

    element = ET.Element(name)

    if isinstance(payload, dict):
        text = payload.get('text')
        if text is not None:
            element.text = str(text)

        for key, value in payload.items():
            if key == 'text':
                continue
            if key.startswith('@'):
                element.set(key[1:], str(value))
                continue
            if isinstance(value, list):
                for item in value:
                    element.append(_dict_to_element(key, item))
            else:
                element.append(_dict_to_element(key, value))
    elif isinstance(payload, list):
        for item in payload:
            element.append(_dict_to_element('item', item))
    elif payload is not None:
        element.text = str(payload)

    return element


def _write_csv(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write `data` to `path` as CSV and return the number of rows.
    """

    rows: list[JSONDict]
    if isinstance(data, list):
        rows = [row for row in data if isinstance(row, dict)]
    else:
        rows = [data]

    if not rows:
        return 0

    fieldnames = sorted({key for row in rows for key in row})
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})

    return len(rows)


def _write_json(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write `data` to `path` as formatted JSON and return record count.
    """

    with path.open('w', encoding='utf-8') as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write('\n')

    return len(data) if isinstance(data, list) else 1


def _write_xml(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write `data` to `path` as XML and return record count.
    """

    if isinstance(data, dict) and len(data) == 1:
        root_name, payload = next(iter(data.items()))
        root_element = _dict_to_element(str(root_name), payload)
    else:
        root_element = _dict_to_element('root', data)

    tree = ET.ElementTree(root_element)
    tree.write(path, encoding='utf-8', xml_declaration=True)

    return len(data) if isinstance(data, list) else 1


# -- File Normalization -- #


def _coerce_file_format(
    file_format: FileFormat | str,
) -> FileFormat:
    """
    Normalize textual file format values to `FileFormat` members.

    Parameters
    ----------
    file_format : FileFormat | str
        File format to normalize.

    Returns
    -------
    FileFormat
        Normalized file format.

    Raises
    ------
    ValueError
        If the file format is not supported.
    """

    if isinstance(file_format, FileFormat):
        return file_format
    try:
        return FileFormat(str(file_format).lower())
    except ValueError as e:
        raise ValueError(f'Unsupported format: {file_format}') from e


def _coerce_http_method(
    method: HttpMethod | str,
) -> HttpMethod:
    """
    Normalize HTTP method input to ``HttpMethod`` values.
    """

    if isinstance(method, HttpMethod):
        return method
    try:
        return HttpMethod(str(method).lower())
    except ValueError as e:
        raise ValueError(f'Unsupported HTTP method: {method}') from e


def _coerce_data_connector_type(
    data_connector_type: DataConnectorType | str,
) -> DataConnectorType:
    """
    Normalize data connector identifiers to `DataConnectorType` members.

    Parameters
    ----------
    data_connector_type : DataConnectorType | str
        Source type to normalize.

    Returns
    -------
    DataConnectorType
        Normalized source type.

    Raises
    ------
    ValueError
        If the source type is not supported.
    """

    if isinstance(data_connector_type, DataConnectorType):
        return data_connector_type
    try:
        return DataConnectorType(str(data_connector_type).lower())
    except ValueError as e:
        raise ValueError(
            f'Invalid data connector type: {data_connector_type}',
        ) from e


def _load_json_from_path(
    path: Path,
) -> JSONData:
    """
    Read JSON content from `path` and validate the structure.
    """

    if not path.exists():
        raise FileNotFoundError(f'File not found: {path}')

    with path.open('r', encoding='utf-8') as handle:
        loaded = json.load(handle)

    if isinstance(loaded, dict):
        return cast(JSONDict, loaded)
    if isinstance(loaded, list):
        if all(isinstance(item, dict) for item in loaded):
            return cast(JSONList, loaded)
        raise ValueError(
            'JSON array must contain only objects (dicts) when loading file',
        )
    raise ValueError(
        'JSON root must be an object or array when loading from file',
    )


def _parse_json_string(
    raw: str,
) -> JSONData:
    """
    Parse JSON data from `raw` text.
    """

    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid data source: {raw}') from e

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
        return _load_json_from_path(source)

    if isinstance(source, str):
        candidate = Path(source)
        if candidate.exists():
            try:
                return _load_json_from_path(candidate)
            except (OSError, json.JSONDecodeError, ValueError):
                # Fall back to treating the string as raw JSON content.
                pass
        return _parse_json_string(source)

    raise TypeError(
        'source must be a mapping, sequence of mappings, path, or JSON string',
    )


# -- File Target -- #


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

    fmt = _coerce_file_format(file_format)
    match fmt:
        case FileFormat.JSON:
            records = _write_json(path, data)
            message = f'Data loaded to {path}'
        case FileFormat.CSV:
            records = _write_csv(path, data)
            message = (
                'No data to write'
                if records == 0
                else f'Data loaded to {path}'
            )
        case FileFormat.XML:
            records = _write_xml(path, data)
            message = f'Data loaded to {path}'
        case _:
            # Ensure exhaustive handling in case new enum members are added.
            raise ValueError(f'Unsupported format: {file_format}')

    return {
        'status': 'success',
        'message': message,
        'records': records,
    }


# -- Database Target (Placeholder) -- #


def load_to_database(
    data: JSONData,
    connection_string: str,
) -> JSONData:
    """
    Load data to a database.

    Notes
    -----
    Placeholder implementation. To enable database loading, install and
    configure database-specific drivers.

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
        If the HTTP request fails or returns an error status.
    ValueError
        If ``method`` is not supported.
    """
    http_method = _coerce_http_method(method)

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
    target_type : {'file', 'database', 'api'}
        Type of target to load to.
    target : StrPath
        Target location (file path, connection string, or URL).
    **kwargs : Any
        Additional arguments; e.g., `format` for files, `method` for APIs.

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
    ttype = _coerce_data_connector_type(target_type)

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

    # `_coerce_target_type` covers invalid entries, but keep explicit guard.
    raise ValueError(f'Invalid target type: {target_type}')
