"""
:mod:`etlplus.file.core` module.

Shared helpers for reading and writing structured and semi-structured data
files.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..enums import FileFormat
from ..enums import infer_file_format_and_compression
from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import StrPath
from . import csv
from . import json
from . import xml
from . import yaml

# SECTION: EXPORTS ========================================================== #


__all__ = ['File']


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class File:
    """
    Convenience wrapper around structured file IO.

    This class encapsulates the one-off helpers in this module as convenient
    instance methods while retaining the original function API for
    backward compatibility (those functions delegate to this class).

    Attributes
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

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self) -> None:
        """
        Auto-detect and set the file format on initialization.

        If no explicit ``file_format`` is provided, attempt to infer it from
        the file path's extension and update :attr:`file_format`. If the
        extension is unknown, the attribute is left as ``None`` and will be
        validated later by :meth:`_ensure_format`.
        """
        # Normalize incoming path (allow str in constructor) to Path.
        if isinstance(self.path, str):
            self.path = Path(self.path)

        if self.file_format is None:
            try:
                self.file_format = self._guess_format()
            except ValueError:
                # Leave as None; _ensure_format() will raise on use if needed.
                pass

    # -- Internal Instance Methods -- #

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

        Returns
        -------
        FileFormat
            The resolved file format.
        """
        return (
            self.file_format
            if self.file_format is not None
            else self._guess_format()
        )

    def _guess_format(self) -> FileFormat:
        """
        Infer the file format from the filename extension.

        Returns
        -------
        FileFormat
            The inferred file format based on the file extension.

        Raises
        ------
        ValueError
            If the extension is unknown or unsupported.
        """
        fmt, compression = infer_file_format_and_compression(self.path)
        if fmt is not None:
            return fmt
        if compression is not None:
            raise ValueError(
                'Cannot infer file format from compressed file '
                f'{self.path!r} with compression {compression.value!r}',
            )
        raise ValueError(
            f'Cannot infer file format from extension {self.path.suffix!r}',
        )

    # -- Instance Methods (Generic API) -- #

    def read(self) -> JSONData:
        """
        Read structured data from :attr:`path` using :attr:`file_format`.

        Returns
        -------
        JSONData
            The structured data read from the file.

        Raises
        ------
        ValueError
            If the resolved file format is unsupported.
        """
        fmt = self._ensure_format()
        match fmt:
            case FileFormat.JSON:
                return self.read_json()
            case FileFormat.CSV:
                return self.read_csv()
            case FileFormat.XML:
                return self.read_xml()
            case FileFormat.YAML:
                return self.read_yaml()
        raise ValueError(f'Unsupported format: {fmt}')

    def write(
        self,
        data: JSONData,
        *,
        root_tag: str = xml.DEFAULT_XML_ROOT,
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

        Returns
        -------
        int
            The number of records written.

        Raises
        ------
        ValueError
            If the resolved file format is unsupported.
        """
        fmt = self._ensure_format()
        match fmt:
            case FileFormat.JSON:
                return self.write_json(data)
            case FileFormat.CSV:
                return self.write_csv(data)
            case FileFormat.XML:
                return self.write_xml(data, root_tag=root_tag)
            case FileFormat.YAML:
                return self.write_yaml(data)
        raise ValueError(f'Unsupported format: {fmt}')

    # -- Instance Methods (CSV) -- #

    def read_csv(self) -> JSONList:
        """
        Load CSV content as a list of dictionaries from :attr:`path`.

        Returns
        -------
        JSONList
            The list of dictionaries read from the CSV file.
        """
        self._assert_exists()

        return csv.read(self.path)

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
        return csv.write(self.path, data)

    # -- Instance Methods (JSON) -- #

    def read_json(self) -> JSONData:
        """
        Load and validate JSON payloads from :attr:`path`.

        Returns
        -------
        JSONData
            The structured data read from the JSON file.
        """
        self._assert_exists()

        return json.read(self.path)

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
        return json.write(self.path, data)

    # -- Instance Methods (XML) -- #

    def read_xml(self) -> JSONDict:
        """
        Parse XML document at :attr:`path` into a nested dictionary.

        Returns
        -------
        JSONDict
            Nested dictionary representation of the XML file.
        """
        self._assert_exists()

        return xml.read(self.path)

    def write_xml(
        self,
        data: JSONData,
        *,
        root_tag: str = xml.DEFAULT_XML_ROOT,
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
        return xml.write(self.path, data, root_tag=root_tag)

    # -- Instance Methods (YAML) -- #

    def read_yaml(self) -> JSONData:
        """
        Load and validate YAML payloads from :attr:`path`.

        Returns
        -------
        JSONData
            The structured data read from the YAML file.
        """
        return yaml.read(self.path)

    def write_yaml(
        self,
        data: JSONData,
    ) -> int:
        """
        Write ``data`` as YAML to :attr:`path` and return record count.

        Parameters
        ----------
        data : JSONData
            Data to write as YAML.

        Returns
        -------
        int
            The number of records written.
        """
        return yaml.write(self.path, data)

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
        Read structured data.

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
        root_tag: str = xml.DEFAULT_XML_ROOT,
    ) -> int:
        """
        Write structured data and count written records.

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
