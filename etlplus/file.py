"""
ETLPlus Files
=======================

Shared helpers for reading and writing structured and semi-structured data
files.
"""
from __future__ import annotations

import csv
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from typing import cast

from .enums import FileFormat
from .types import JSONData
from .types import JSONDict
from .types import JSONList


# SECTION: PUBLIC API ======================================================= #


__all__ = [
    'read_csv',
    'read_json',
    'read_structured_file',
    'read_xml',
    'write_csv',
    'write_json',
    'write_structured_file',
    'write_xml',
]


# SECTION: PROTECTED CONSTANTS ============================================== #


_DEFAULT_XML_ROOT = 'root'


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _element_to_dict(
    element: ET.Element,
) -> JSONDict:
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


def _dict_to_element(
    name: str,
    payload: Any,
) -> ET.Element:
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


# SECTION: FUNCTIONS ======================================================== #


# -- Structured Files -- #


def read_structured_file(
    path: Path,
    file_format: FileFormat,
) -> JSONData:
    """
    Read structured data from ``path`` using the requested format.

    Parameters
    ----------
    path : Path
        The file path to read from.
    file_format : FileFormat
        The format of the file.

    Returns
    -------
    JSONData
        The structured data read from the file.
    """

    match file_format:
        case FileFormat.JSON:
            return read_json(path)
        case FileFormat.CSV:
            return read_csv(path)
        case FileFormat.XML:
            return read_xml(path)
    raise ValueError(f'Unsupported format: {file_format}')


def write_structured_file(
    path: Path,
    data: JSONData,
    file_format: FileFormat,
) -> int:
    """
    Write ``data`` to ``path`` using ``file_format`` and return count.

    Parameters
    ----------
    path : Path
        The file path to write to.
    data : JSONData
        The structured data to write.
    file_format : FileFormat
        The format of the file.

    Returns
    -------
    int
        The number of records written.
    """

    match file_format:
        case FileFormat.JSON:
            return write_json(path, data)
        case FileFormat.CSV:
            return write_csv(path, data)
        case FileFormat.XML:
            return write_xml(path, data)
    raise ValueError(f'Unsupported format: {file_format}')


# -- CSV Files -- #


def read_csv(
    path: Path,
) -> JSONList:
    """
    Load CSV content as a list of dictionaries.

    Parameters
    ----------
    path : Path
        The file path to read from.

    Returns
    -------
    JSONList
        The list of dictionaries read from the CSV file.
    """

    if not path.exists():
        raise FileNotFoundError(f'File not found: {path}')

    with path.open('r', encoding='utf-8', newline='') as handle:
        reader: csv.DictReader[str] = csv.DictReader(handle)
        rows: JSONList = []
        for row in reader:
            if not any(row.values()):
                continue
            rows.append(cast(JSONDict, dict(row)))
    return rows


def write_csv(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write ``data`` to ``path`` as CSV and return the number of rows.

    Parameters
    ----------
    path : Path
        The file path to write to.
    data : JSONData
        The structured data to write.

    Returns
    -------
    int
        The number of rows written.
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


# -- JSON Files -- #


def read_json(
    path: Path,
) -> JSONData:
    """
    Load and validate JSON payloads from ``path``.

    Parameters
    ----------
    path : Path
        The file path to read from.

    Returns
    -------
    JSONData
        The structured data read from the JSON file.
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
        raise TypeError(
            'JSON array must contain only objects (dicts) when loading file',
        )
    raise TypeError(
        'JSON root must be an object or an array of objects when loading file',
    )


def write_json(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write ``data`` to ``path`` as formatted JSON and return record count.

    Parameters
    ----------
    path : Path
        The file path to write to.
    data : JSONData
        The structured data to write.

    Returns
    -------
    int
        The number of records written.
    """

    with path.open('w', encoding='utf-8') as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write('\n')

    return len(data) if isinstance(data, list) else 1


# -- XML Files -- #


def read_xml(
    path: Path,
) -> JSONDict:
    """
    Parse XML documents into nested dictionaries.

    Parameters
    ----------
    path : Path
        The file path to read from.

    Returns
    -------
    JSONDict
        The nested dictionary representation of the XML document.
    """

    if not path.exists():
        raise FileNotFoundError(f'File not found: {path}')

    tree = ET.parse(path)
    root = tree.getroot()

    return {root.tag: _element_to_dict(root)}


def write_xml(
    path: Path,
    data: JSONData,
    *,
    root_tag: str = _DEFAULT_XML_ROOT,
) -> int:
    """
    Write ``data`` to ``path`` as XML and return record count.

    Parameters
    ----------
    path : Path
        The file path to write to.
    data : JSONData
        The structured data to write.
    root_tag : str, optional
        The root tag name for the XML document, by default _DEFAULT_XML_ROOT

    Returns
    -------
    int
        The number of records written.
    """

    if isinstance(data, dict) and len(data) == 1:
        root_name, payload = next(iter(data.items()))
        root_element = _dict_to_element(str(root_name), payload)
    else:
        root_element = _dict_to_element(root_tag, data)

    tree = ET.ElementTree(root_element)
    tree.write(path, encoding='utf-8', xml_declaration=True)

    return len(data) if isinstance(data, list) else 1
