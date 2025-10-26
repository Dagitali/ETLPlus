"""
ETLPlus Data Extraction
=======================

Tools to extract data from files, databases, and REST APIs.
"""
from __future__ import annotations

import csv
import enum
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from typing import cast
from typing import TypeAlias

import requests


# SECTION: TYPE ALIASES ===================================================== #


JSONDict: TypeAlias = dict[str, Any]
JSONList: TypeAlias = list[JSONDict]
JSONData: TypeAlias = JSONDict | JSONList


# SECTION: CLASSES ========================================================== #


class FileFormat(enum.StrEnum):
    """
    Supported file formats for extraction.
    """

    JSON = 'json'
    CSV = 'csv'
    XML = 'xml'


class SourceType(enum.StrEnum):
    """
    Supported data source types.
    """

    FILE = 'file'
    DATABASE = 'database'
    API = 'api'


# SECTION: PROTECTED FUNCTIONS ============================================== #


# -- File Extraction -- #


def _element_to_dict(
    element: ET.Element,
) -> JSONDict:
    """
    Convert an XML element (and its children) to `JSONDict`.
    """

    result: JSONDict = {}
    text = (element.text or '').strip()
    if text:
        result['text'] = text

    for child in element:
        child_data = _element_to_dict(child)
        tag = child.tag
        if tag in result:
            existing = result[tag]
            if isinstance(existing, list):
                existing.append(child_data)
            else:
                result[tag] = [existing, child_data]
        else:
            result[tag] = child_data

    for key, value in element.attrib.items():
        if key in result:
            result[f'@{key}'] = value
        else:
            result[key] = value
    return result


def _read_csv(
    path: Path,
) -> JSONList:
    """
    Load CSV content as a list of dictionaries.
    """

    with path.open('r', encoding='utf-8', newline='') as handle:
        reader: csv.DictReader[str] = csv.DictReader(handle)
        rows: JSONList = []
        for row in reader:
            if not any(row.values()):
                continue
            rows.append(cast(JSONDict, dict(row)))
    return rows


def _read_json(
    path: Path,
) -> JSONData:
    """
    Load and validate JSON payloads from `path`.
    """

    with path.open('r', encoding='utf-8') as handle:
        loaded = json.load(handle)

    if isinstance(loaded, dict):
        return cast(JSONDict, loaded)
    if isinstance(loaded, list):
        if all(isinstance(item, dict) for item in loaded):
            return cast(JSONList, loaded)
        raise TypeError('JSON array must contain only objects (dicts)')
    raise TypeError('JSON root must be an object or an array of objects')


def _read_xml(
    path: Path,
) -> JSONDict:
    """
    Parse XML documents into nested dictionaries.
    """

    tree = ET.parse(path)
    root = tree.getroot()
    return {root.tag: _element_to_dict(root)}


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


def _coerce_source_type(
    source_type: SourceType | str,
) -> SourceType:
    """
    Normalize textual source type inputs to `SourceType` members.

    Parameters
    ----------
    source_type : SourceType | str
        Source type to normalize.

    Returns
    -------
    SourceType
        Normalized source type.

    Raises
    ------
    ValueError
        If the source type is not supported.
    """

    if isinstance(source_type, SourceType):
        return source_type
    try:
        return SourceType(str(source_type).lower())
    except ValueError as e:
        raise ValueError(f'Invalid source type: {source_type}') from e


# SECTION: FUNCTIONS ======================================================== #


# -- File Extraction -- #


def extract_from_file(
    file_path: str,
    file_format: FileFormat | str = FileFormat.JSON,
) -> JSONData:
    """
    Extract (semi-)structured data from a file.

    Parameters
    ----------
    file_path : str
        Path to the file to read.
    file_format : {'json', 'csv', 'xml'}, optional
        File format to parse. Defaults to `'json'`.

    Returns
    -------
    JSONData
        Parsed data as a mapping or a list of mappings.

    Raises
    ------
    FileNotFoundError
        If `file_path` does not exist.
    ValueError
        If `file_format` is not supported.
    TypeError
        If parsed JSON is not an object or an array of objects.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f'File not found: {path}')

    fmt = _coerce_file_format(file_format)
    match fmt:
        case FileFormat.JSON:
            return _read_json(path)
        case FileFormat.CSV:
            return _read_csv(path)
        case FileFormat.XML:
            return _read_xml(path)

    # The `match` statement is exhaustive, but mypy/pyright expect a return.
    raise ValueError(f'Unsupported format: {file_format}')


# -- Database Extraction (Placeholder) -- #


def extract_from_database(
    connection_string: str,
) -> JSONList:
    """
    Extract data from a database.

    Notes
    -----
    Placeholder implementation. To enable database extraction, install and
    configure database-specific drivers and query logic.

    Parameters
    ----------
    connection_string : str
        Database connection string.

    Returns
    -------
    JSONList
        Informational message payload.
    """
    return [
        {
            'message': 'Database extraction not yet implemented',
            'connection_string': connection_string,
            'note': (
                'Install database-specific drivers to enable this feature'
            ),
        },
    ]


# -- API Extraction -- #


def extract_from_api(
    url: str,
    **kwargs: Any,
) -> JSONData:
    """
    Extract data from a REST API.

    Parameters
    ----------
    url : str
        API endpoint URL.
    **kwargs : Any
        Extra arguments forwarded to `requests.get` (e.g., `timeout`). To
        use a pre-configured `requests.Session`, provide it via `session`.

    Returns
    -------
    JSONData
        Parsed JSON payload, or a fallback object with raw text.

    Raises
    ------
    requests.RequestException
        If the HTTP request fails or a non-2xx status is returned.
    """
    # Apply a conservative timeout to guard against hanging requests.
    timeout = kwargs.pop('timeout', 10.0)
    session = kwargs.pop('session', None)
    if session is not None:
        get_method = getattr(session, 'get', None)
        if not callable(get_method):
            raise TypeError("session must expose a callable 'get' method")
        response = get_method(url, timeout=timeout, **kwargs)
    else:
        response = requests.get(url, timeout=timeout, **kwargs)
    response.raise_for_status()

    content_type = response.headers.get('content-type', '').lower()
    if 'application/json' in content_type:
        try:
            payload: Any = response.json()
        except ValueError:
            # Malformed JSON despite content-type; fall back to text
            return {
                'content': response.text,
                'content_type': content_type,
            }
        if isinstance(payload, dict):
            return cast(JSONDict, payload)
        if isinstance(payload, list):
            if all(isinstance(x, dict) for x in payload):
                return cast(JSONList, payload)
            # Coerce non-dict array items into objects for consistency
            return [{'value': x} for x in payload]
        # Fallback: wrap scalar JSON
        return {'value': payload}

    return {'content': response.text, 'content_type': content_type}


# -- Orchestrator -- #


def extract(
    source_type: SourceType | str,
    source: str,
    **kwargs: Any,
) -> JSONData:
    """
    Extract data from a source.

    Parameters
    ----------
    source_type : SourceType | str
        Type of source to extract from.
    source : str
        Source location (file path, connection string, or API URL).
    **kwargs : Any
        Additional arguments; for files, `format` may be provided.

    Returns
    -------
    JSONData
        Extracted data.

    Raises
    ------
    ValueError
        If `source_type` is not one of the supported values.
    """
    stype = _coerce_source_type(source_type)
    if stype is SourceType.FILE:
        file_format = kwargs.pop(
            'format', kwargs.pop('file_format', FileFormat.JSON),
        )
        return extract_from_file(source, file_format)
    if stype is SourceType.DATABASE:
        return extract_from_database(source)
    if stype is SourceType.API:
        return extract_from_api(source, **kwargs)

    # `_coerce_source_type` covers invalid entries, but keep explicit guard.
    raise ValueError(f'Invalid source type: {source_type}')
