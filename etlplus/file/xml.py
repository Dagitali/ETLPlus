"""
:mod:`etlplus.file.xml` module.

Helpers for reading/writing Extensible Markup Language (XML) files.

Notes
-----
- An XML file is a markup language file that uses tags to define elements.
- Common cases:
    - Configuration files.
    - Data interchange between systems.
    - Document formatting.
- Rule of thumb:
    - If the file follows the XML specification, use this module for
        reading and writing.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from ..types import JSONData
from ..types import JSONDict
from ..utils import count_records

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: CONSTANTS ======================================================== #


DEFAULT_XML_ROOT = 'root'


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _dict_to_element(
    name: str,
    payload: Any,
) -> ET.Element:
    """
    Convert a dictionary-like payload into an XML element.

    Parameters
    ----------
    name : str
        Name of the XML element.
    payload : Any
        The data to include in the XML element.

    Returns
    -------
    ET.Element
        The constructed XML element.
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


def _element_to_dict(
    element: ET.Element,
) -> JSONDict:
    """
    Convert an XML element into a nested dictionary.

    Parameters
    ----------
    element : ET.Element
        XML element to convert.

    Returns
    -------
    JSONDict
        Nested dictionary representation of the XML element.
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


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONDict:
    """
    Read XML content from *path*.

    Parameters
    ----------
    path : Path
        Path to the XML file on disk.

    Returns
    -------
    JSONDict
        Nested dictionary representation of the XML file.
    """
    tree = ET.parse(path)
    root = tree.getroot()

    return {root.tag: _element_to_dict(root)}


def write(
    path: Path,
    data: JSONData,
    *,
    root_tag: str,
) -> int:
    """
    Write *data* to XML at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the XML file on disk.
    data : JSONData
        Data to write as XML.
    root_tag : str
        Root tag name to use when writing XML files.

    Returns
    -------
    int
        The number of records written to the XML file.
    """
    if isinstance(data, dict) and len(data) == 1:
        root_name, payload = next(iter(data.items()))
        root_element = _dict_to_element(str(root_name), payload)
    else:
        root_element = _dict_to_element(root_tag, data)

    tree = ET.ElementTree(root_element)
    tree.write(path, encoding='utf-8', xml_declaration=True)

    return count_records(data)
