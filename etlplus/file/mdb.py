"""
:mod:`etlplus.file.mdb` module.

Stub helpers for reading/writing newer Microsoft Access database (MDB) files
(not implemented yet).

Notes
-----
- An MDB file is a proprietary database file format used by Microsoft Access
    2003 and earlier.
- Common cases:
    - Storing relational data for small to medium-sized applications.
    - Desktop database applications.
    - Data management for non-enterprise solutions.
- Rule of thumb:
    - If the file follows the MDB specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from . import stub
from ._io import coerce_path

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read MDB content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the MDB file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the MDB file.
    """
    return stub.read(path, format_name='MDB')


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to MDB at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the MDB file on disk.
    data : JSONData
        Data to write as MDB. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the MDB file.
    """
    path = coerce_path(path)
    return stub.write(path, data, format_name='MDB')
