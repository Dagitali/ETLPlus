"""
:mod:`etlplus.file.zip` module.

Helpers for reading/writing ZIP files.
"""

from __future__ import annotations

import tempfile
import zipfile
from pathlib import Path

from ..types import JSONData
from ..types import JSONDict
from .enums import CompressionFormat
from .enums import FileFormat
from .enums import infer_file_format_and_compression

# SECTION: EXPORTS ========================================================== #


__all__ = [
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


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONData:
    """
    Read ZIP content from *path* and parse the inner payload(s).

    Parameters
    ----------
    path : Path
        Path to the ZIP file on disk.

    Returns
    -------
    JSONData
        Parsed payload.

    Raises
    ------
    ValueError
        If the ZIP archive is empty.
    """
    with zipfile.ZipFile(path, 'r') as archive:
        entries = [entry for entry in archive.infolist() if not entry.is_dir()]
        if not entries:
            raise ValueError(f'ZIP archive is empty: {path}')

        if len(entries) == 1:
            entry = entries[0]
            fmt = _resolve_format(entry.filename)
            payload = _extract_payload(entry, archive)
            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_path = Path(tmpdir) / Path(entry.filename).name
                tmp_path.write_bytes(payload)
                from .core import File

                return File(tmp_path, fmt).read()

        results: JSONDict = {}
        for entry in entries:
            fmt = _resolve_format(entry.filename)
            payload = _extract_payload(entry, archive)
            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_path = Path(tmpdir) / Path(entry.filename).name
                tmp_path.write_bytes(payload)
                from .core import File

                results[entry.filename] = File(tmp_path, fmt).read()
        return results


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to ZIP at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the ZIP file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.
    """
    fmt = _resolve_format(path.name)
    inner_name = Path(path.name).with_suffix('').name

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / inner_name
        from .core import File

        count = File(tmp_path, fmt).write(data)
        payload = tmp_path.read_bytes()

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(
        path,
        'w',
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        archive.writestr(inner_name, payload)

    return count
