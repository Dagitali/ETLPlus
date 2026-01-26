"""
:mod:`etlplus.file.accdb` module.

Helpers for reading/writing newer Microsoft Access database (ACCDB) files.

Notes
-----
- An ACCDB file is a proprietary database file format used by Microsoft Access
    2007 and later.
- Common cases:
    - Storing relational data for small to medium-sized applications.
    - Desktop database applications.
    - Data management for non-enterprise solutions.
- Rule of thumb:
    - If the file follows the ACCDB specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import JSONList
from . import stub

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read ACCDB content from *path*.

    Parameters
    ----------
    path : Path
        Path to the ACCDB file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ACCDB file.
    """
    return stub.read(path, format_name='ACCDB')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to ACCDB at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the ACCDB file on disk.
    data : JSONData
        Data to write as ACCDB. Should be a list of dictionaries or a single
        dictionary.

    Returns
    -------
    int
        The number of rows written to the ACCDB file.
    """
    return stub.write(path, data, format_name='ACCDB')
