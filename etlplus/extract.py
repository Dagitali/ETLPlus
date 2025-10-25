"""Data extraction module for ETLPlus.

This module provides functionality to extract data from various sources:
- Files (JSON, CSV, XML)
- Databases (via connection strings)
- REST APIs
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

# -----------------------------
# Type aliases
# -----------------------------
JSONDict: TypeAlias = dict[str, Any]
JSONList: TypeAlias = list[JSONDict]
JSONData: TypeAlias = JSONDict | JSONList


# -----------------------------
# File extraction
# -----------------------------

def extract_from_file(
    file_path: str,
    file_format: Literal['json', 'csv', 'xml'] = 'json',
) -> JSONData:
    """Extract data from a file.

    Args:
        file_path: Path to the file
        file_format: File format (json, csv, xml)

    Returns:
        Extracted data as dictionary or list of dictionaries

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If format is unsupported
        TypeError: If parsed content isn't a dict or a list of dicts
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
            """Convert XML element to a dictionary."""
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


# -----------------------------
# Database extraction (placeholder)
# -----------------------------

def extract_from_database(
    connection_string: str,
) -> JSONList:
    """Extract data from a database (placeholder implementation)."""
    return [
        {
            'message': 'Database extraction not yet implemented',
            'connection_string': connection_string,
            'note': (
                'Install database-specific drivers to enable this feature'
            ),
        },
    ]


# -----------------------------
# API extraction
# -----------------------------

def extract_from_api(
    url: str,
    **kwargs: Any,
) -> JSONData:
    """Extract data from a REST API.

    Args:
        url: API endpoint URL
        **kwargs: Additional args forwarded to ``requests.get``

    Returns:
        Extracted data from API response.
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


# -----------------------------
# Orchestrator
# -----------------------------

def extract(
    source_type: Literal['file', 'database', 'api'],
    source: str,
    **kwargs: Any,
) -> JSONData:
    """Extract data from a source.

    Args:
        source_type: Type of source (file, database, api)
        source: Source location
        **kwargs: Additional arguments (e.g., format for files)

    Returns:
        Extracted data
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
