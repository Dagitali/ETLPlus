"""
:mod:`etlplus.file.stub` module.

Helpers for reading/writing stubbed files.
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
    Read stubbed content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the stubbed file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the stubbed file.

    Raises
    ------
    NotImplementedError
        Always, since this is a stub implementation.
    """
    raise NotImplementedError('Stubbed read is not implemented yet')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write ``data`` to stubbed file at ``path`` and return record count.

    Parameters
    ----------
    path : Path
        Path to the stubbed file on disk.
    data : JSONData
        Data to write as stubbed file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the stubbed file.

    Raises
    ------
    NotImplementedError
        Always, since this is a stub implementation.
    """
    raise NotImplementedError('Stubbed write is not implemented yet')
