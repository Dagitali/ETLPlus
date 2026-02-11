"""
:mod:`etlplus.file.gz` module.

Helpers for reading/writing GZ files.
"""

from __future__ import annotations

import gzip
from pathlib import Path

from ..types import JSONData
from ..types import StrPath
from ._core_dispatch import read_payload_with_core
from ._core_dispatch import write_payload_with_core
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import warn_deprecated_module_io
from .base import ArchiveWrapperFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import CompressionFormat
from .enums import FileFormat
from .enums import infer_file_format_and_compression

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'GzFile',
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _resolve_format(
    path: StrPath,
) -> FileFormat:
    """
    Resolve the inner file format from a .gz filename.

    Parameters
    ----------
    path : StrPath
        Path to the GZ file on disk.

    Returns
    -------
    FileFormat
        The inferred inner file format.

    Raises
    ------
    ValueError
        If the file format cannot be inferred from the filename.
    """
    fmt, compression = infer_file_format_and_compression(path)
    if compression is not CompressionFormat.GZ:
        raise ValueError(f'Not a gzip file: {path}')
    if fmt is None:
        raise ValueError(
            f'Cannot infer file format from compressed file {path!r}',
        )
    return fmt


# SECTION: CLASSES ========================================================== #


class GzFile(ArchiveWrapperFileHandlerABC):
    """
    Handler implementation for GZ files.
    """

    # -- Class Attributes -- #

    format = FileFormat.GZ
    default_inner_name = 'payload'

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read GZ content from *path* and parse the inner payload.

        Parameters
        ----------
        path : Path
            Path to the GZ file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed payload.
        """
        fmt = _resolve_format(path)
        payload = self.read_inner_bytes(path, options=options)
        return read_payload_with_core(
            fmt=fmt,
            payload=payload,
            filename=f'payload.{fmt.value}',
        )

    def read_inner_bytes(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> bytes:
        """
        Read and return decompressed inner payload bytes.

        Parameters
        ----------
        path : Path
            Path to the GZ file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        bytes
            Decompressed payload bytes.
        """
        _ = options
        with gzip.open(path, 'rb') as handle:
            return handle.read()

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to GZ at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the GZ file on disk.
        data : JSONData
            Data to write.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of records written.
        """
        fmt = _resolve_format(path)
        count, payload = write_payload_with_core(
            fmt=fmt,
            data=data,
            filename=f'payload.{fmt.value}',
        )

        self.write_inner_bytes(path, payload, options=options)
        return count

    def write_inner_bytes(
        self,
        path: Path,
        payload: bytes,
        *,
        options: WriteOptions | None = None,
    ) -> None:
        """
        Compress and write inner payload bytes.

        Parameters
        ----------
        path : Path
            Path to the GZ file on disk.
        payload : bytes
            Raw inner payload bytes.
        options : WriteOptions | None, optional
            Optional write parameters.
        """
        _ = options
        ensure_parent_dir(path)
        with gzip.open(path, 'wb') as handle:
            handle.write(payload)


# SECTION: INTERNAL CONSTANTS =============================================== #

_GZ_HANDLER = GzFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Deprecated wrapper. Use ``GzFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the GZ file on disk.

    Returns
    -------
    JSONData
        Parsed payload.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _GZ_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``GzFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the GZ file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.
    """
    warn_deprecated_module_io(__name__, 'write')
    return _GZ_HANDLER.write(coerce_path(path), data)
