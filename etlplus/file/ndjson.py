"""
:mod:`etlplus.file.ndjson` module.

Stub helpers for NDJSON read/write.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData

# SECTION: EXPORTS ========================================================== #


def read(path: Path) -> JSONData:
    """
    Read NDJSON content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the NDJSON file on disk.

    Returns
    -------
    JSONData
        Parsed payload.

    Raises
    ------
    NotImplementedError
        NDJSON :func:`read` is not implemented yet.
    """
    raise NotImplementedError('NDJSON read is not implemented yet')


def write(path: Path, data: JSONData) -> int:
    """
    Write ``data`` to NDJSON at ``path``.

    Parameters
    ----------
    path : Path
        Path to the NDJSON file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.

    Raises
    ------
    NotImplementedError
        NDJSON :func:`write` is not implemented yet.
    """
    raise NotImplementedError('NDJSON write is not implemented yet')
