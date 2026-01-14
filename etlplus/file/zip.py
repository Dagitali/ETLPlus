"""
:mod:`etlplus.file.zip` module.

Stub helpers for ZIP read/write.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData

# SECTION: EXPORTS ========================================================== #


def read(path: Path) -> JSONData:
    """
    Read ZIP content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the ZIP file on disk.

    Returns
    -------
    JSONData
        Parsed payload.
    """
    raise NotImplementedError('ZIP read is not implemented yet')


def write(path: Path, data: JSONData) -> int:
    """
    Write ``data`` to ZIP at ``path``.

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
    raise NotImplementedError('ZIP write is not implemented yet')
