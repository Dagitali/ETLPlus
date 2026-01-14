"""
:mod:`etlplus.file.parquet` module.

Stub helpers for PARQUET read/write.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData

# SECTION: EXPORTS ========================================================== #


def read(path: Path) -> JSONData:
    """
    Read PARQUET content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the PARQUET file on disk.

    Returns
    -------
    JSONData
        Parsed payload.

    Raises
    ------
    NotImplementedError
        PARQUET :func:`read` is not implemented yet.
    """
    raise NotImplementedError('PARQUET read is not implemented yet')


def write(path: Path, data: JSONData) -> int:
    """
    Write ``data`` to PARQUET at ``path``.

    Parameters
    ----------
    path : Path
        Path to the PARQUET file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.

    Raises
    ------
    NotImplementedError
        PARQUET :func:`write` is not implemented yet.
    """
    raise NotImplementedError('PARQUET write is not implemented yet')
