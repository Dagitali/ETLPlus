"""ETLPlus Data Extraction
=======================

Tools to extract data from files, databases, and REST APIs.
"""
from __future__ import annotations

import csv
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from typing import cast
from typing import Literal
from typing import TypeAlias

import requests


# SECTION: TYPE ALIASES ===================================================== #


JSONDict: TypeAlias = dict[str, Any]
JSONList: TypeAlias = list[JSONDict]
JSONData: TypeAlias = JSONDict | JSONList


# SECTION: FUNCTIONS ======================================================== #


# -- File extraction -- #


def extract_from_file(
    file_path: str,
    file_format: Literal['json', 'csv', 'xml'] = 'json',
) -> JSONData:
    """
    Extract data from a file.

    Parameters
    ----------
    file_path : str
        Path to the file to read.
    file_format : {'json', 'csv', 'xml'}, optional
        File format to parse. Defaults to ``'json'``.

    Returns
    -------
    dict[str, Any] | list[dict[str, Any]]
        Parsed data as a mapping or a list of mappings.

    Raises
    ------
    FileNotFoundError
        If ``file_path`` does not exist.
    ValueError
        If ``file_format`` is not supported.
    TypeError
        If parsed JSON is not an object or an array of objects.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    fmt = file_format.lower()
    if fmt == 'json':
        with path.open('r', encoding='utf-8') as f:
            loaded = json.load(f)
        if isinstance(loaded, dict):
            return cast(JSONDict, loaded)
        if isinstance(loaded, list):
            if all(isinstance(x, dict) for x in loaded):
                return cast(JSONList, loaded)
            raise TypeError(
                'JSON array must contain only objects (dicts)',
            )
        raise TypeError('JSON root must be an object or an array of objects')

    if fmt == 'csv':
        with path.open('r', encoding='utf-8') as f:
            reader: csv.DictReader[str] = csv.DictReader(f)
            rows: JSONList = []
            for row in reader:
                # Convert row (dict[str, str]) to JSONDict (dict[str, Any])
                rows.append(cast(JSONDict, dict(row)))
        return rows

    if fmt == 'xml':
        tree = ET.parse(path)
        root = tree.getroot()

        def element_to_dict(element: ET.Element) -> JSONDict:
            """
            Convert an XML element to a dictionary.

            Parameters
            ----------
            element : xml.etree.ElementTree.Element
                Root element to convert.

            Returns
            -------
            dict[str, Any]
                A dictionary representing the element, its attributes,
                children, and text.
            """
            result: JSONDict = {}
            text = (element.text or '').strip()
            if text:
                result['text'] = text
            for child in element:
                child_data = element_to_dict(child)
                tag = child.tag
                if tag in result:
                    if not isinstance(result[tag], list):
                        result[tag] = [result[tag]]  # type: ignore[assignment]
                    cast(list[JSONDict], result[tag]).append(child_data)
                else:
                    result[tag] = child_data
            # include attributes
            for k, v in element.attrib.items():
                result[k] = v
            return result

        return {root.tag: element_to_dict(root)}

    raise ValueError(f"Unsupported format: {file_format}")


# -- Database extraction (placeholder) -- #


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
    list[dict[str, Any]]
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


# -- API extraction -- #


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
        Extra arguments forwarded to ``requests.get`` (e.g., ``timeout``).

    Returns
    -------
    dict[str, Any] | list[dict[str, Any]]
        Parsed JSON payload, or a fallback object with raw text.

    Raises
    ------
    requests.RequestException
        If the HTTP request fails or a non-2xx status is returned.
    """
    response = requests.get(url, **kwargs)
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
    source_type: Literal['file', 'database', 'api'],
    source: str,
    **kwargs: Any,
) -> JSONData:
    """
    Extract data from a source.

    Parameters
    ----------
    source_type : {'file', 'database', 'api'}
        Type of source to extract from.
    source : str
        Source location (file path, connection string, or API URL).
    **kwargs : Any
        Additional arguments; for files, ``format`` may be provided.

    Returns
    -------
    dict[str, Any] | list[dict[str, Any]]
        Extracted data.

    Raises
    ------
    ValueError
        If ``source_type`` is not one of the supported values.
    """
    if source_type == 'file':
        file_format = cast(
            Literal['json', 'csv', 'xml'], kwargs.get('format', 'json'),
        )
        return extract_from_file(source, file_format)
    if source_type == 'database':
        return extract_from_database(source)
    if source_type == 'api':
        return extract_from_api(source, **kwargs)
    raise ValueError(f"Invalid source type: {source_type}")
