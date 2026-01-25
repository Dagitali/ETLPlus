"""
:mod:`etlplus.file.gz` module.

Helpers for reading/writing GZ files.
"""

from __future__ import annotations

import gzip
import tempfile
from pathlib import Path

from ..types import JSONData
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
    path: Path,
) -> FileFormat:
    """
    Resolve the inner file format from a .gz filename.

    Parameters
    ----------
    path : Path
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


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONData:
    """
    Read GZ content from *path* and parse the inner payload.

    Parameters
    ----------
    path : Path
        Path to the GZ file on disk.

    Returns
    -------
    JSONData
        Parsed payload.
    """
    fmt = _resolve_format(path)
    with gzip.open(path, 'rb') as handle:
        payload = handle.read()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / f'payload.{fmt.value}'
        tmp_path.write_bytes(payload)
        from .core import File

        return File(tmp_path, fmt).read()


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to GZ at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the GZ file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.
    """
    fmt = _resolve_format(path)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / f'payload.{fmt.value}'
        from .core import File

        count = File(tmp_path, fmt).write(data)
        payload = tmp_path.read_bytes()

    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, 'wb') as handle:
        handle.write(payload)

    return count
