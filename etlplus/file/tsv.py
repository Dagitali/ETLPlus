"""
:mod:`etlplus.file.tsv` module.

Stub helpers for TSV read/write.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData

# SECTION: EXPORTS ========================================================== #


def read(path: Path) -> JSONData:
    """
    Read TSV content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the TSV file on disk.

    Returns
    -------
    JSONData
        Parsed payload.

    Raises
    ------
    NotImplementedError
        TSV :func:`read` is not implemented yet.
    """
    raise NotImplementedError('TSV read is not implemented yet')


def write(path: Path, data: JSONData) -> int:
    """
    Write ``data`` to TSV at ``path``.

    Parameters
    ----------
    path : Path
        Path to the TSV file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.

    Raises
    ------
    NotImplementedError
        TSV :func:`write` is not implemented yet.
    """
    raise NotImplementedError('TSV write is not implemented yet')
