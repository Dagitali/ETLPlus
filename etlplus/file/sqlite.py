"""
:mod:`etlplus.file.sqlite` module.

Helpers for reading/writing SQLite database (SQLITE) files.

Notes
-----
- A SQLITE file is a self-contained, serverless database file format used by
    SQLite.
- Common cases:
    - Lightweight database applications.
    - Embedded database solutions.
    - Mobile and desktop applications requiring local data storage.
- Rule of thumb:
    - If the file follows the SQLITE specification, use this module for reading
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
    Read SQLITE content from *path*.

    Parameters
    ----------
    path : Path
        Path to the SQLITE file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the SQLITE file.
    """
    return stub.read(path, format_name='SQLITE')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to SQLITE at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the SQLITE file on disk.
    data : JSONData
        Data to write as SQLITE. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the SQLITE file.
    """
    return stub.write(path, data, format_name='SQLITE')
