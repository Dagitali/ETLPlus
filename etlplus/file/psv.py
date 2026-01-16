"""
:mod:`etlplus.file.psv` module.

Helpers for reading/writing PSV (pipe-separated values) files.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import JSONList
from . import stub

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
    """
    return stub.read(path, format_name='PSV')


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
    """
    return stub.write(path, data, format_name='PSV')
