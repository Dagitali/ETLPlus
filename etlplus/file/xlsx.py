"""
:mod:`etlplus.file.xlsx` module.

Stub helpers for XLSX read/write.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData

# SECTION: EXPORTS ========================================================== #


def read(path: Path) -> JSONData:
    """
    Read XLSX content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the XLSX file on disk.

    Returns
    -------
    JSONData
        Parsed payload.

    Raises
    ------
    NotImplementedError
        XLSX :func:`read` is not implemented yet.
    """
    raise NotImplementedError('XLSX read is not implemented yet')


def write(path: Path, data: JSONData) -> int:
    """
    Write ``data`` to XLSX at ``path``.

    Parameters
    ----------
    path : Path
        Path to the XLSX file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.

    Raises
    ------
    NotImplementedError
        XLSX :func:`write` is not implemented yet.
    """
    raise NotImplementedError('XLSX write is not implemented yet')
