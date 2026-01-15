"""
:mod:`etlplus.file.psv` module.

Helpers for reading/writing PSV (pipe-separated values) files.
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
    Read PSV content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the PSV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the PSV file.

    Raises
    ------
    NotImplementedError
        Always, since this is a stub implementation.
    """
    raise NotImplementedError('PSV read is not implemented yet')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write ``data`` to PSV file at ``path`` and return record count.

    Parameters
    ----------
    path : Path
        Path to the PSV file on disk.
    data : JSONData
        Data to write as PSV file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the PSV file.

    Raises
    ------
    NotImplementedError
        Always, since this is a stub implementation.
    """
    raise NotImplementedError('PSV write is not implemented yet')
