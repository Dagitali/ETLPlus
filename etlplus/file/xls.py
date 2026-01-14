"""
:mod:`etlplus.file.xls` module.

Stub helpers for XLS read/write.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData

# SECTION: EXPORTS ========================================================== #


def read(path: Path) -> JSONData:
    """
    Read XLS content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the XLS file on disk.

    Returns
    -------
    JSONData
        Parsed payload.

    Raises
    ------
    NotImplementedError
        XLS :func:`read` is not implemented yet.
    """
    raise NotImplementedError('XLS read is not implemented yet')


def write(path: Path, data: JSONData) -> int:
    """
    Write ``data`` to XLS at ``path``.

    Parameters
    ----------
    path : Path
        Path to the XLS file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.

    Raises
    ------
    NotImplementedError
        XLS :func:`write` is not implemented yet.
    """
    raise NotImplementedError('XLS write is not implemented yet')
