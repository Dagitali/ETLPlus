"""
:mod:`etlplus.file.zip` module.

Helpers for reading/writing ZIP files.
"""

from __future__ import annotations

import zipfile
from pathlib import Path
from typing import cast

from ..utils._types import JSONData
from ..utils._types import JSONDict
from ._archive import infer_archive_payload_format
from ._core_dispatch import read_payload_with_core
from ._core_dispatch import write_payload_with_core
from ._enums import CompressionFormat
from ._enums import FileFormat
from ._io import ensure_parent_dir
from .base import ArchiveWrapperFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ZipFile',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _archive_entries(
    archive: zipfile.ZipFile,
    *,
    path: Path,
) -> list[zipfile.ZipInfo]:
    """
    Return non-directory archive entries or raise when the archive is empty.

    Parameters
    ----------
    archive : zipfile.ZipFile
        The opened ZIP archive.
    path : Path
        Path to the ZIP file on disk, used for error messages.

    Returns
    -------
    list[zipfile.ZipInfo]
        List of non-directory archive entries.

    Raises
    ------
    ValueError
        If the ZIP archive is empty.
    """
    entries = [entry for entry in archive.infolist() if not entry.is_dir()]
    if not entries:
        raise ValueError(f'ZIP archive is empty: {path}')
    return entries


def _find_entry(
    entries: list[zipfile.ZipInfo],
    inner_name: str,
) -> zipfile.ZipInfo:
    """
    Return one member matching *inner_name* or raise a clear error.

    Parameters
    ----------
    entries : list[zipfile.ZipInfo]
        List of non-directory archive entries.
    inner_name : str
        The name of the archive member to find.

    Returns
    -------
    zipfile.ZipInfo
        The matching archive entry.

    Raises
    ------
    ValueError
        If no matching archive entry is found.
    """
    for entry in entries:
        if entry.filename == inner_name:
            return entry
    raise ValueError(f'ZIP archive member not found: {inner_name!r}')


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
    """
    fmt = infer_archive_payload_format(
        filename,
        allowed_compressions=(None, CompressionFormat.ZIP),
        compression_error=f'Unexpected compression in archive: {filename}',
    )
    return cast(FileFormat, fmt)


def _decode_entry_with_core(
    archive: zipfile.ZipFile,
    entry: zipfile.ZipInfo,
) -> JSONData:
    """
    Decode one archive member payload through :mod:`etlplus.file._core`.

    Parameters
    ----------
    archive : zipfile.ZipFile
        The opened ZIP archive.
    entry : zipfile.ZipInfo
        The ZIP archive entry.

    Returns
    -------
    JSONData
        The decoded payload.
    """
    fmt = _resolve_format(entry.filename)
    payload = _extract_payload(entry, archive)
    return read_payload_with_core(
        filename=entry.filename,
        fmt=fmt,
        payload=payload,
    )


# SECTION: CLASSES ========================================================== #


class ZipFile(ArchiveWrapperFileHandlerABC):
    """Handler implementation for ZIP files."""

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
        """
        inner_name = self.inner_name_from_options(options)
        with zipfile.ZipFile(path, 'r') as archive:
            entries = _archive_entries(archive, path=path)
            if inner_name is not None:
                return _decode_entry_with_core(
                    archive,
                    _find_entry(entries, inner_name),
                )

            if len(entries) == 1:
                return _decode_entry_with_core(archive, entries[0])

            results: JSONDict = {}
            for entry in entries:
                results[entry.filename] = _decode_entry_with_core(
                    archive,
                    entry,
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
        inner_name = self.inner_name_from_options(options)
        with zipfile.ZipFile(path, 'r') as archive:
            entries = _archive_entries(archive, path=path)
            if inner_name is not None:
                return _extract_payload(
                    _find_entry(entries, inner_name),
                    archive,
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
        _ = infer_archive_payload_format(
            path,
            allowed_compressions=(None, CompressionFormat.ZIP),
            compression_error=f'Unexpected compression in archive: {path}',
            require_format=False,
        )

        default_inner_name = Path(path.name).with_suffix('').name
        inner_name = self.inner_name_from_options(
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
        inner_name = self.inner_name_from_options(
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
