"""
:mod:`etlplus.file.txt` module.

Stub helpers for TXT read/write.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData

# SECTION: EXPORTS ========================================================== #


def read(path: Path) -> JSONData:
    """
    Read TXT content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the TXT file on disk.

    Returns
    -------
    JSONData
        Parsed payload.

    Raises
    ------
    NotImplementedError
        TXT :func:`read` is not implemented yet.
    """
    raise NotImplementedError('TXT read is not implemented yet')


def write(path: Path, data: JSONData) -> int:
    """
    Write ``data`` to TXT at ``path``.

    Parameters
    ----------
    path : Path
        Path to the TXT file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.

    Raises
    ------
    NotImplementedError
        TXT :func:`write` is not implemented yet.
    """
    raise NotImplementedError('TXT write is not implemented yet')
