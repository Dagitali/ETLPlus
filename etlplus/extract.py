"""
etlplus.extract module.

Helpers to extract data from files, databases, and REST APIs.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import cast

import requests

from .enums import coerce_data_connector_type
from .enums import coerce_file_format
from .enums import DataConnectorType
from .enums import FileFormat
from .file import File
from .types import JSONData
from .types import JSONDict
from .types import JSONList
from .types import StrPath


# SECTION: FUNCTIONS ======================================================== #


# -- File Extraction -- #


def extract_from_file(
    file_path: StrPath,
    file_format: FileFormat | str = FileFormat.JSON,
) -> JSONData:
    """
    Extract (semi-)structured data from a local file.

    Parameters
    ----------
    file_path : StrPath
        Source file path.
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
        If `file_format` is not one of the supported formats.
    TypeError
        If parsed JSON is not an object or an array of objects.
    """
    path = Path(file_path)
    fmt = coerce_file_format(file_format)

    # Let file module perform existence and format validation.
    return File(path, fmt).read()


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


# -- REST API Extraction -- #


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
        If the HTTP request fails or returns an error (i.e., non-2xx) status.
    ValueError
        If the HTTP response content is not valid JSON.
    """
    # Apply a conservative timeout to guard against hanging requests.
    timeout = kwargs.pop('timeout', 10.0)
    session = kwargs.pop('session', None)
    if session is not None:
        get_method = getattr(session, 'get', None)
        if not callable(get_method):
            raise TypeError(
                'Session must expose a callable "get" method',
            )
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


# -- Orchestration -- #


def extract(
    source_type: DataConnectorType | str,
    source: StrPath,
    **kwargs: Any,
) -> JSONData:
    """
    Extract data from a source.

    Parameters
    ----------
    source_type : DataConnectorType | str
        Type of source to extract from.
    source : StrPath
        Source location (file path, connection string, or API URL).
    **kwargs : Any
        Additional arguments (e.g., `format` for files, `method` for APIs).

    Returns
    -------
    JSONData
        Extracted data.

    Raises
    ------
    ValueError
        If `source_type` is not one of the supported values.
    """
    stype = coerce_data_connector_type(source_type)

    if stype is DataConnectorType.FILE:
        file_format = kwargs.pop(
            'format', kwargs.pop('file_format', FileFormat.JSON),
        )
        return extract_from_file(source, file_format)

    if stype is DataConnectorType.DATABASE:
        return extract_from_database(str(source))

    if stype is DataConnectorType.API:
        return extract_from_api(str(source), **kwargs)

    # `coerce_data_connector_type` covers invalid entries, but keep explicit
    # guard.
    raise ValueError(f'Invalid source type: {source_type}')
