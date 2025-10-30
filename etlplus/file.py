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
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import cast

from .enums import FileFormat
from .types import JSONData
from .types import JSONDict
from .types import JSONList
from .types import StrPath


# SECTION: PUBLIC API ======================================================= #


__all__ = [
    'File',
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
# Map common filename extensions to FileFormat (used for inference)
_EXT_TO_FORMAT: dict[str, FileFormat] = {
    'csv': FileFormat.CSV,
    'json': FileFormat.JSON,
    'xml': FileFormat.XML,
    # NOTE: YAML is defined in FileFormat but not implemented in this module.
}


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _count_records(data: JSONData) -> int:
    """Return a consistent record count for JSON-like data payloads.

    Lists are treated as multiple records; dicts as a single record.
    """

    return len(data) if isinstance(data, list) else 1


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


# SECTION: CLASS ============================================================ #


@dataclass(slots=True)
class File:
    """
    Convenience wrapper around structured file IO.

    This class encapsulates the one-off helpers in this module as convenient
    instance methods while retaining the original function API for
    backward compatibility (those functions delegate to this class).

    Parameters
    ----------
    path : Path
        Path to the file on disk.
    file_format : FileFormat | None, optional
        Explicit format. If omitted, the format is inferred from the file
        extension (``.csv``, ``.json``, or ``.xml``).
    """

    # -- Attributes -- #

    path: Path
    file_format: FileFormat | None = None

    # -- Magic Methods (Constructors) -- #

    def __post_init__(self) -> None:
        """
        Auto-detect and set the file format on initialization.

        If no explicit ``file_format`` is provided, attempt to infer it from
        the file path's extension and update :attr:`file_format`. If the
        extension is unknown, the attribute is left as ``None`` and will be
        validated later by :meth:`_ensure_format`.
        """

        if self.file_format is None:
            try:
                self.file_format = self._guess_format()
            except ValueError:
                # Leave as None; _ensure_format() will raise on use if needed.
                pass

    # -- Protected Instance Methods -- #

    def _assert_exists(self) -> None:
        """
        Raise FileNotFoundError if :attr:`path` does not exist.

        This centralizes existence checks across multiple read methods.
        """

        if not self.path.exists():
            raise FileNotFoundError(f'File not found: {self.path}')

    def _ensure_format(self) -> FileFormat:
        """
        Resolve the active format, guessing from extension if needed.
        """

        return self.file_format \
            if self.file_format is not None \
            else self._guess_format()

    def _guess_format(self) -> FileFormat:
        """
        Infer the file format from the filename extension.

        Raises
        ------
        ValueError
            If the extension is unknown or unsupported.
        """

        ext = self.path.suffix.lstrip('.').casefold()
        try:
            return _EXT_TO_FORMAT[ext]
        except KeyError as e:
            raise ValueError(
                'Cannot infer file format from extension '
                f'{self.path.suffix!r}',
            ) from e
    # -- Instance Methods (Generic API) -- #

    def read(self) -> JSONData:
        """
        Read structured data from :attr:`path` using :attr:`file_format`.
        """

        fmt = self._ensure_format()
        match fmt:
            case FileFormat.JSON:
                return self.read_json()
            case FileFormat.CSV:
                return self.read_csv()
            case FileFormat.XML:
                return self.read_xml()
        raise ValueError(f'Unsupported format: {fmt}')

    def write(
        self,
        data: JSONData,
        *,
        root_tag: str = _DEFAULT_XML_ROOT,
    ) -> int:
        """
        Write ``data`` to :attr:`path` using :attr:`file_format`.

        Parameters
        ----------
        data : JSONData
            Data to write to the file.
        root_tag : str, optional
            Root tag name to use when writing XML files. Defaults to
            ``'root'``.
        """

        fmt = self._ensure_format()
        match fmt:
            case FileFormat.JSON:
                return self.write_json(data)
            case FileFormat.CSV:
                return self.write_csv(data)
            case FileFormat.XML:
                return self.write_xml(data, root_tag=root_tag)
        raise ValueError(f'Unsupported format: {fmt}')

    # -- Instance Methods (CSV) -- #

    def read_csv(self) -> JSONList:
        """
        Load CSV content as a list of dictionaries from :attr:`path`.
        """

        self._assert_exists()

        with self.path.open('r', encoding='utf-8', newline='') as handle:
            reader: csv.DictReader[str] = csv.DictReader(handle)
            rows: JSONList = []
            for row in reader:
                if not any(row.values()):
                    continue
                rows.append(cast(JSONDict, dict(row)))
        return rows

    def write_csv(
        self,
        data: JSONData,
    ) -> int:
        """
        Write CSV rows to :attr:`path` and return the number of rows.

        Parameters
        ----------
        data : JSONData
            Data to write as CSV. Should be a list of dictionaries or a
            single dictionary.

        Returns
        -------
        int
            The number of rows written to the CSV file.
        """

        rows: list[JSONDict]
        if isinstance(data, list):
            rows = [row for row in data if isinstance(row, dict)]
        else:
            rows = [data]

        if not rows:
            return 0

        fieldnames = sorted({key for row in rows for key in row})
        with self.path.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {field: row.get(field) for field in fieldnames},
                )

        return len(rows)

    # -- Instance Methods (JSON) -- #

    def read_json(self) -> JSONData:
        """
        Load and validate JSON payloads from :attr:`path`.
        """

        self._assert_exists()

        with self.path.open('r', encoding='utf-8') as handle:
            loaded = json.load(handle)

        if isinstance(loaded, dict):
            return cast(JSONDict, loaded)
        if isinstance(loaded, list):
            if all(isinstance(item, dict) for item in loaded):
                return cast(JSONList, loaded)
            raise TypeError(
                'JSON array must contain only objects (dicts) '
                'when loading file',
            )
        raise TypeError(
            'JSON root must be an object or an array of objects '
            'when loading file',
        )

    def write_json(
        self,
        data: JSONData,
    ) -> int:
        """
        Write ``data`` as formatted JSON to :attr:`path`.

        Parameters
        ----------
        data : JSONData
            Data to serialize as JSON.

        Returns
        -------
        int
            The number of records written to the JSON file.
        """

        with self.path.open('w', encoding='utf-8') as handle:
            json.dump(data, handle, indent=2, ensure_ascii=False)
            handle.write('\n')

        return _count_records(data)

    # -- Instance Methods (XML) -- #

    def read_xml(self) -> JSONDict:
        """
        Parse XML document at :attr:`path` into a nested dictionary.
        """

        self._assert_exists()

        tree = ET.parse(self.path)
        root = tree.getroot()

        return {root.tag: _element_to_dict(root)}

    def write_xml(
        self,
        data: JSONData,
        *,
        root_tag: str = _DEFAULT_XML_ROOT,
    ) -> int:
        """
        Write ``data`` as XML to :attr:`path` and return record count.

        Parameters
        ----------
        data : JSONData
            Data to write as XML.
        root_tag : str, optional
            Root tag name to use when writing XML files. Defaults to
            ``'root'``.

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
        tree.write(self.path, encoding='utf-8', xml_declaration=True)

        return _count_records(data)

    # -- Class Methods -- #

    @classmethod
    def from_path(
        cls,
        path: StrPath,
        *,
        file_format: FileFormat | str | None = None,
    ) -> File:
        """
        Create a :class:`File` from any path-like and optional format.

        Parameters
        ----------
        path : StrPath
            Path to the file on disk.
        file_format : FileFormat | str | None, optional
            Explicit format. If omitted, the format is inferred from the file
            extension (``.csv``, ``.json``, or ``.xml``).

        Returns
        -------
        File
            The constructed :class:`File` instance.
        """

        resolved = Path(path)
        ff: FileFormat | None
        if isinstance(file_format, str):
            ff = FileFormat.coerce(file_format)
        else:
            ff = file_format

        return cls(resolved, ff)

    @classmethod
    def read_file(
        cls,
        path: StrPath,
        file_format: FileFormat | str | None = None,
    ) -> JSONData:
        """
        Convenience: ``File.read_file(path, fmt)`` → read structured data.

        Parameters
        ----------
        path : StrPath
            Path to the file on disk.
        file_format : FileFormat | str | None, optional
            Explicit format. If omitted, the format is inferred from the file
            extension (``.csv``, ``.json``, or ``.xml``).

        Returns
        -------
        JSONData
            The structured data read from the file.
        """

        return cls.from_path(path, file_format=file_format).read()

    @classmethod
    def write_file(
        cls,
        path: StrPath,
        data: JSONData,
        file_format: FileFormat | str | None = None,
        *,
        root_tag: str = _DEFAULT_XML_ROOT,
    ) -> int:
        """
        Convenience: ``File.write_file(path, data, fmt)`` → write and count.

        Parameters
        ----------
        path : StrPath
            Path to the file on disk.
        data : JSONData
            Data to write to the file.
        file_format : FileFormat | str | None, optional
            Explicit format. If omitted, the format is inferred from the file
            extension (``.csv``, ``.json``, or ``.xml``).
        root_tag : str, optional
            Root tag name to use when writing XML files. Defaults to
            ``'root'``.

        Returns
        -------
        int
            The number of records written to the file.
        """

        return cls.from_path(path, file_format=file_format).write(
            data,
            root_tag=root_tag,
        )


# SECTION: FUNCTIONS ======================================================== #


# -- Structured Files -- #


def read_structured_file(
    path: Path,
    file_format: FileFormat,
) -> JSONData:
    return File(path, file_format).read()


def write_structured_file(
    path: Path,
    data: JSONData,
    file_format: FileFormat,
) -> int:
    return File(path, file_format).write(data)


# -- CSV Files -- #


def read_csv(
    path: Path,
) -> JSONList:
    return File(path, FileFormat.CSV).read_csv()


def write_csv(
    path: Path,
    data: JSONData,
) -> int:
    return File(path, FileFormat.CSV).write_csv(data)


# -- JSON Files -- #


def read_json(
    path: Path,
) -> JSONData:
    return File(path, FileFormat.JSON).read_json()


def write_json(
    path: Path,
    data: JSONData,
) -> int:
    return File(path, FileFormat.JSON).write_json(data)


# -- XML Files -- #


def read_xml(
    path: Path,
) -> JSONDict:
    return File(path, FileFormat.XML).read_xml()


def write_xml(
    path: Path,
    data: JSONData,
    *,
    root_tag: str = _DEFAULT_XML_ROOT,
) -> int:
    return File(path, FileFormat.XML).write_xml(data, root_tag=root_tag)
