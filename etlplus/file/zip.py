"""
:mod:`etlplus.file.zip` module.

Helpers for reading/writing ZIP files.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

from ..types import JSONData
from ..types import JSONDict
from ..types import StrPath
from ._core_dispatch import read_payload_with_core
from ._core_dispatch import write_payload_with_core
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import ensure_parent_dir
from .base import ArchiveWrapperFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import CompressionFormat
from .enums import FileFormat
from .enums import infer_file_format_and_compression

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ZipFile',
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _resolve_format(
    filename: str,
) -> FileFormat:
    """
    Resolve the inner file format from a filename.

    Parameters
    ----------
    filename : str
        The name of the file inside the ZIP archive.

    Returns
    -------
    FileFormat
        The inferred inner file format.

    Raises
    ------
    ValueError
        If the file format cannot be inferred from the filename.
    """
    fmt, compression = infer_file_format_and_compression(filename)
    if compression is not None and compression is not CompressionFormat.ZIP:
        raise ValueError(f'Unexpected compression in archive: {filename}')
    if fmt is None:
        raise ValueError(
            f'Cannot infer file format from compressed file {filename!r}',
        )
    return fmt


def _extract_payload(
    entry: zipfile.ZipInfo,
    archive: zipfile.ZipFile,
) -> bytes:
    """
    Extract an archive entry into memory.

    Parameters
    ----------
    entry : zipfile.ZipInfo
        The ZIP archive entry.
    archive : zipfile.ZipFile
        The opened ZIP archive.

    Returns
    -------
    bytes
        The raw payload.
    """
    with archive.open(entry, 'r') as handle:
        return handle.read()


# SECTION: CLASSES ========================================================== #


class ZipFile(ArchiveWrapperFileHandlerABC):
    """
    Handler implementation for ZIP files.
    """

    # -- Class Attributes -- #

    format = FileFormat.ZIP
    default_inner_name = 'payload'

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read ZIP content from *path* and parse the inner payload(s).

        Parameters
        ----------
        path : Path
            Path to the ZIP file on disk.
        options : ReadOptions | None, optional
            Optional read parameters. ``inner_name`` can select a single
            archive member.

        Returns
        -------
        JSONData
            Parsed payload.

        Raises
        ------
        ValueError
            If the ZIP archive is empty.
            If ``inner_name`` is provided and does not match an archive member.
        """
        inner_name = self.inner_name_from_read_options(options)
        with zipfile.ZipFile(path, 'r') as archive:
            entries = [
                entry for entry in archive.infolist() if not entry.is_dir()
            ]
            if not entries:
                raise ValueError(f'ZIP archive is empty: {path}')

            if inner_name is not None:
                for entry in entries:
                    if entry.filename == inner_name:
                        fmt = _resolve_format(entry.filename)
                        payload = _extract_payload(entry, archive)
                        return read_payload_with_core(
                            filename=entry.filename,
                            fmt=fmt,
                            payload=payload,
                        )
                raise ValueError(
                    f'ZIP archive member not found: {inner_name!r}',
                )

            if len(entries) == 1:
                entry = entries[0]
                fmt = _resolve_format(entry.filename)
                payload = _extract_payload(entry, archive)
                return read_payload_with_core(
                    filename=entry.filename,
                    fmt=fmt,
                    payload=payload,
                )

            results: JSONDict = {}
            for entry in entries:
                fmt = _resolve_format(entry.filename)
                payload = _extract_payload(entry, archive)
                results[entry.filename] = read_payload_with_core(
                    filename=entry.filename,
                    fmt=fmt,
                    payload=payload,
                )
            return results

    def read_inner_bytes(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> bytes:
        """
        Read a single archive member and return its bytes.

        Parameters
        ----------
        path : Path
            Path to the ZIP file on disk.
        options : ReadOptions | None, optional
            Optional read parameters. ``inner_name`` can select a specific
            member.

        Returns
        -------
        bytes
            Inner member payload bytes.

        Raises
        ------
        ValueError
            If the ZIP archive is empty.
            If multiple members are present and no ``inner_name`` is provided.
            If ``inner_name`` does not match any archive member.
        """
        inner_name = self.inner_name_from_read_options(options)
        with zipfile.ZipFile(path, 'r') as archive:
            entries = [
                entry for entry in archive.infolist() if not entry.is_dir()
            ]
            if not entries:
                raise ValueError(f'ZIP archive is empty: {path}')
            if inner_name is not None:
                for entry in entries:
                    if entry.filename == inner_name:
                        return _extract_payload(entry, archive)
                raise ValueError(
                    f'ZIP archive member not found: {inner_name!r}',
                )
            if len(entries) != 1:
                raise ValueError(
                    'ZIP archive contains multiple members; specify '
                    '"inner_name" to select one',
                )
            return _extract_payload(entries[0], archive)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to ZIP at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the ZIP file on disk.
        data : JSONData
            Data to write.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of records written.

        Raises
        ------
        ValueError
            If the output path indicates a non-ZIP compression format.
            If the inner file format cannot be inferred from the provided
            options.
        """
        _fmt, output_compression = infer_file_format_and_compression(path)
        if (
            output_compression is not None
            and output_compression is not CompressionFormat.ZIP
        ):
            raise ValueError(f'Unexpected compression in archive: {path}')

        default_inner_name = Path(path.name).with_suffix('').name
        inner_name = self.inner_name_from_write_options(
            options,
            default=default_inner_name,
        )
        if inner_name is None:  # pragma: no cover
            raise ValueError('ZIP inner archive member name is required')
        fmt = _resolve_format(inner_name)

        count, payload = write_payload_with_core(
            filename=inner_name,
            fmt=fmt,
            data=data,
        )

        self.write_inner_bytes(
            path,
            payload,
            options=WriteOptions(inner_name=inner_name),
        )
        return count

    def write_inner_bytes(
        self,
        path: Path,
        payload: bytes,
        *,
        options: WriteOptions | None = None,
    ) -> None:
        """
        Write payload bytes into a ZIP archive member.

        Parameters
        ----------
        path : Path
            Path to the ZIP file on disk.
        payload : bytes
            Raw member payload bytes.
        options : WriteOptions | None, optional
            Optional write parameters. ``inner_name`` can override the archive
            member name.

        Raises
        ------
        ValueError
            If ``inner_name`` is not provided and cannot be inferred from the
            ZIP filename.
        """
        inner_name = self.inner_name_from_write_options(
            options,
            default=self.default_inner_name,
        )
        if inner_name is None:  # pragma: no cover - guarded by default
            raise ValueError('ZIP write requires an inner archive member name')
        ensure_parent_dir(path)
        with zipfile.ZipFile(
            path,
            'w',
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            archive.writestr(inner_name, payload)


# SECTION: INTERNAL CONSTANTS =============================================== #

_ZIP_HANDLER = ZipFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Deprecated wrapper. Use ``ZipFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the ZIP file on disk.

    Returns
    -------
    JSONData
        Parsed payload.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _ZIP_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``ZipFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the ZIP file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _ZIP_HANDLER.write,
    )
