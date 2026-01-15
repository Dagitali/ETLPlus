"""
:mod:`etlplus.file.dat` module.

Helpers for reading/writing DAT (data) files.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import JSONList

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read DAT content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the DAT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the DAT file.

    Raises
    ------
    NotImplementedError
        Always, since this is a stub implementation.
    """
    raise NotImplementedError('DAT read is not implemented yet')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write ``data`` to DAT file at ``path`` and return record count.

    Parameters
    ----------
    path : Path
        Path to the DAT file on disk.
    data : JSONData
        Data to write as DAT file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the DAT file.

    Raises
    ------
    NotImplementedError
        Always, since this is a stub implementation.
    """
    raise NotImplementedError('DAT write is not implemented yet')
