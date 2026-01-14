"""
:mod:`etlplus.file.gz` module.

Stub helpers for GZ read/write.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData

# SECTION: EXPORTS ========================================================== #


def read(path: Path) -> JSONData:
    """
    Read GZ content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the GZ file on disk.

    Returns
    -------
    JSONData
        Parsed payload.

    Raises
    ------
    NotImplementedError
        GZ :func:`read` is not implemented yet.
    """
    raise NotImplementedError('GZ read is not implemented yet')


def write(path: Path, data: JSONData) -> int:
    """
    Write ``data`` to GZ at ``path``.

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

    Raises
    ------
    NotImplementedError
        GZ :func:`write` is not implemented yet.
    """
    raise NotImplementedError('GZ write is not implemented yet')
