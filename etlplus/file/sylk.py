"""
:mod:`etlplus.file.sylk` module.

Helpers for reading/writing Symbolic Link (SYLK) data files.

Notes
-----
- A SYLK file is a text-based file format used to represent spreadsheet
    data, including cell values, formulas, and formatting.
- Common cases:
    - Storing spreadsheet data in a human-readable format.
    - Exchanging data between different spreadsheet applications.
- Rule of thumb:
    - If you need to work with SYLK files, use this module for reading
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
    Read SYLK content from *path*.

    Parameters
    ----------
    path : Path
        Path to the SYLK file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the SYLK file.
    """
    return stub.read(path, format_name='SYLK')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to SYLK file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the SYLK file on disk.
    data : JSONData
        Data to write as SYLK file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the SYLK file.
    """
    return stub.write(path, data, format_name='SYLK')
