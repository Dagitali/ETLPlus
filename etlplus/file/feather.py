"""
:mod:`etlplus.file.feather` module.

Stub helpers for FEATHER read/write.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData

# SECTION: EXPORTS ========================================================== #


def read(path: Path) -> JSONData:
    """
    Read FEATHER content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the FEATHER file on disk.

    Returns
    -------
    JSONData
        Parsed payload.

    Raises
    ------
    NotImplementedError
        FEATHER :func:`read` is not implemented yet.
    """
    raise NotImplementedError('FEATHER read is not implemented yet')


def write(path: Path, data: JSONData) -> int:
    """
    Write ``data`` to FEATHER at ``path``.

    Parameters
    ----------
    path : Path
        Path to the FEATHER file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.

    Raises
    ------
    NotImplementedError
        FEATHER :func:`write` is not implemented yet.
    """
    raise NotImplementedError('FEATHER write is not implemented yet')
