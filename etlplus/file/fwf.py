"""
:mod:`etlplus.file.fwf` module.

Helpers for reading/writing FWF (fixed-width fields) files.
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
    Read FWF content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the FWF file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the FWF file.

    Raises
    ------
    NotImplementedError
        Always, since this is a stub implementation.
    """
    raise NotImplementedError('FWF read is not implemented yet')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write ``data`` to FWF file at ``path`` and return record count.

    Parameters
    ----------
    path : Path
        Path to the FWF file on disk.
    data : JSONData
        Data to write as FWF file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the FWF file.

    Raises
    ------
    NotImplementedError
        Always, since this is a stub implementation.
    """
    raise NotImplementedError('FWF write is not implemented yet')
