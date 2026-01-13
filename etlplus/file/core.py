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
        self._assert_exists()
        fmt = self._ensure_format()
        match fmt:
            case FileFormat.CSV:
                return csv.read(self.path)
            case FileFormat.JSON:
                return json.read(self.path)
            case FileFormat.XML:
                return xml.read(self.path)
            case FileFormat.YAML:
                return yaml.read(self.path)
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
            case FileFormat.CSV:
                return csv.write(self.path, data)
            case FileFormat.JSON:
                return json.write(self.path, data)
            case FileFormat.XML:
                return xml.write(self.path, data, root_tag=root_tag)
            case FileFormat.YAML:
                return yaml.write(self.path, data)
        raise ValueError(f'Unsupported format: {fmt}')

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
