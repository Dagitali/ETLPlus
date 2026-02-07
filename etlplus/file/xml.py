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
from ..types import StrPath
from ..utils import count_records
from ._io import coerce_path
from ._io import ensure_parent_dir
from .base import ReadOptions
from .base import SemiStructuredTextFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'XmlFile',
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
        result[f'@{key}'] = value
    return result


# SECTION: CLASSES ========================================================== #


class XmlFile(SemiStructuredTextFileHandlerABC):
    """
    Handler implementation for XML files.
    """

    # -- Class Attributes -- #

    format = FileFormat.XML

    # -- Instance Methods -- #

    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize *data* to XML text.

        Parameters
        ----------
        data : JSONData
            Payload to serialize.
        options : WriteOptions | None, optional
            Optional write parameters. ``root_tag`` determines the XML root
            when *data* does not provide a single-root mapping.

        Returns
        -------
        str
            Serialized XML text.
        """
        root_tag = (
            options.root_tag if options is not None else DEFAULT_XML_ROOT
        )
        if isinstance(data, dict) and len(data) == 1:
            root_name, payload = next(iter(data.items()))
            root_element = _dict_to_element(str(root_name), payload)
        else:
            root_element = _dict_to_element(root_tag, data)
        return ET.tostring(
            root_element,
            encoding='unicode',
            method='xml',
        )

    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse XML *text* into nested dictionary payload.

        Parameters
        ----------
        text : str
            XML payload as text.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed payload.
        """
        _ = options
        root = ET.fromstring(text)
        return {root.tag: _element_to_dict(root)}

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONDict:
        """
        Read and return XML content from *path*.

        Parameters
        ----------
        path : Path
            Path to the XML file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONDict
            Nested dictionary representation of the XML file.
        """
        _ = options
        tree = ET.parse(path)
        root = tree.getroot()
        return {root.tag: _element_to_dict(root)}

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to XML at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the XML file on disk.
        data : JSONData
            Data to write as XML.
        options : WriteOptions | None, optional
            Optional write parameters. ``root_tag`` determines the XML root
            when *data* does not provide a single-root mapping.

        Returns
        -------
        int
            The number of records written to the XML file.
        """
        root_tag = (
            options.root_tag if options is not None else DEFAULT_XML_ROOT
        )
        if isinstance(data, dict) and len(data) == 1:
            root_name, payload = next(iter(data.items()))
            root_element = _dict_to_element(str(root_name), payload)
        else:
            root_element = _dict_to_element(root_tag, data)

        tree = ET.ElementTree(root_element)
        ensure_parent_dir(path)
        tree.write(path, encoding='utf-8', xml_declaration=True)
        return count_records(data)


# SECTION: INTERNAL CONSTANTS ============================================== #


_XML_HANDLER = XmlFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONDict:
    """
    Read and return XML content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the XML file on disk.

    Returns
    -------
    JSONDict
        Nested dictionary representation of the XML file.
    """
    return _XML_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
    *,
    root_tag: str,
) -> int:
    """
    Write *data* to XML at *path* and return record count.

    Parameters
    ----------
    path : StrPath
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
    return _XML_HANDLER.write(
        coerce_path(path),
        data,
        options=WriteOptions(root_tag=root_tag),
    )
