"""
:mod:`etlplus.file.core` module.

Shared helpers for reading and writing structured and semi-structured data
files.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..types import JSONData
from . import avro
from . import csv
from . import dat
from . import feather
from . import fwf
from . import gz
from . import json
from . import ndjson
from . import orc
from . import parquet
from . import psv
from . import stub
from . import tab
from . import tsv
from . import txt
from . import xls
from . import xlsx
from . import xml
from . import yaml
from . import zip as zip_
from .enums import FileFormat
from .enums import infer_file_format_and_compression

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
        extension (``.csv``, ``.json``, etc.).

    Parameters
    ----------
    path : StrPath
        Path to the file on disk.
    file_format : FileFormat | str | None, optional
        Explicit format. If omitted, the format is inferred from the file
        extension (``.csv``, ``.json``, etc.).
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
        self.path = Path(self.path)
        self.file_format = self._coerce_format(self.file_format)
        if self.file_format is None:
            self.file_format = self._maybe_guess_format()

    # -- Internal Instance Methods -- #

    def _assert_exists(self) -> None:
        """
        Raise FileNotFoundError if :attr:`path` does not exist.

        This centralizes existence checks across multiple read methods.
        """
        if not self.path.exists():
            raise FileNotFoundError(f'File not found: {self.path}')

    def _coerce_format(
        self,
        file_format: FileFormat | str | None,
    ) -> FileFormat | None:
        """
        Normalize the file format input.

        Parameters
        ----------
        file_format : FileFormat | str | None
            File format specifier. Strings are coerced into
            :class:`FileFormat`.

        Returns
        -------
        FileFormat | None
            A normalized file format, or ``None`` when unspecified.
        """
        if file_format is None or isinstance(file_format, FileFormat):
            return file_format
        return FileFormat.coerce(file_format)

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

    def _maybe_guess_format(self) -> FileFormat | None:
        """
        Try to infer the format, returning ``None`` if it cannot be inferred.

        Returns
        -------
        FileFormat | None
            The inferred format, or ``None`` if inference fails.
        """
        try:
            return self._guess_format()
        except ValueError:
            # Leave as None; _ensure_format() will raise on use if needed.
            return None

    # -- Instance Methods -- #

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
            case FileFormat.AVRO:
                return avro.read(self.path)
            case FileFormat.CSV:
                return csv.read(self.path)
            case FileFormat.DAT:
                return dat.read(self.path)
            case FileFormat.FEATHER:
                return feather.read(self.path)
            case FileFormat.FWF:
                return fwf.read(self.path)
            case FileFormat.GZ:
                return gz.read(self.path)
            case FileFormat.JSON:
                return json.read(self.path)
            case FileFormat.NDJSON:
                return ndjson.read(self.path)
            case FileFormat.ORC:
                return orc.read(self.path)
            case FileFormat.PARQUET:
                return parquet.read(self.path)
            case FileFormat.PSV:
                return psv.read(self.path)
            case FileFormat.STUB:
                return stub.read(self.path)
            case FileFormat.TAB:
                return tab.read(self.path)
            case FileFormat.TSV:
                return tsv.read(self.path)
            case FileFormat.TXT:
                return txt.read(self.path)
            case FileFormat.XLS:
                return xls.read(self.path)
            case FileFormat.XLSX:
                return xlsx.read(self.path)
            case FileFormat.XML:
                return xml.read(self.path)
            case FileFormat.YAML:
                return yaml.read(self.path)
            case FileFormat.ZIP:
                return zip_.read(self.path)
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
            case FileFormat.AVRO:
                return avro.write(self.path, data)
            case FileFormat.CSV:
                return csv.write(self.path, data)
            case FileFormat.DAT:
                return dat.write(self.path, data)
            case FileFormat.FEATHER:
                return feather.write(self.path, data)
            case FileFormat.FWF:
                return fwf.write(self.path, data)
            case FileFormat.GZ:
                return gz.write(self.path, data)
            case FileFormat.JSON:
                return json.write(self.path, data)
            case FileFormat.NDJSON:
                return ndjson.write(self.path, data)
            case FileFormat.ORC:
                return orc.write(self.path, data)
            case FileFormat.PARQUET:
                return parquet.write(self.path, data)
            case FileFormat.PSV:
                return psv.write(self.path, data)
            case FileFormat.STUB:
                return stub.write(self.path, data)
            case FileFormat.TAB:
                return tab.write(self.path, data)
            case FileFormat.TSV:
                return tsv.write(self.path, data)
            case FileFormat.TXT:
                return txt.write(self.path, data)
            case FileFormat.XLS:
                return xls.write(self.path, data)
            case FileFormat.XLSX:
                return xlsx.write(self.path, data)
            case FileFormat.XML:
                return xml.write(self.path, data, root_tag=root_tag)
            case FileFormat.YAML:
                return yaml.write(self.path, data)
            case FileFormat.ZIP:
                return zip_.write(self.path, data)
        raise ValueError(f'Unsupported format: {fmt}')
