"""
:mod:`etlplus.file.orc` module.

Stub helpers for ORC read/write.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData

# SECTION: EXPORTS ========================================================== #


def read(path: Path) -> JSONData:
    """
    Read ORC content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the ORC file on disk.

    Returns
    -------
    JSONData
        Parsed payload.

    Raises
    ------
    NotImplementedError
        ORC :func:`read` is not implemented yet.
    """
    raise NotImplementedError('ORC read is not implemented yet')


def write(path: Path, data: JSONData) -> int:
    """
    Write ``data`` to ORC at ``path``.

    Parameters
    ----------
    path : Path
        Path to the ORC file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.

    Raises
    ------
    NotImplementedError
        ORC :func:`write` is not implemented yet.
    """
    raise NotImplementedError('ORC write is not implemented yet')
