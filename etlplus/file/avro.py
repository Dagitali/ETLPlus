"""
:mod:`etlplus.file.avro` module.

Stub helpers for AVRO read/write.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData

# SECTION: EXPORTS ========================================================== #


def read(path: Path) -> JSONData:
    """
    Read AVRO content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the AVRO file on disk.

    Returns
    -------
    JSONData
        Parsed payload.

    Raises
    ------
    NotImplementedError
        AVRO :func:`read` is not implemented yet.
    """
    raise NotImplementedError('AVRO read is not implemented yet')


def write(path: Path, data: JSONData) -> int:
    """
    Write ``data`` to AVRO at ``path``.

    Parameters
    ----------
    path : Path
        Path to the AVRO file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.

    Raises
    ------
    NotImplementedError
        AVRO :func:`write` is not implemented yet.
    """
    raise NotImplementedError('AVRO write is not implemented yet')
