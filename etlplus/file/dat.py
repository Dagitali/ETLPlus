"""
:mod:`etlplus.file.dat` module.

Helpers for reading/writing data (DAT) files.

Notes
-----
- A “DAT-formatted” file is a generic data file that may use various
    delimiters or fixed-width formats.
- Common cases:
    - Delimited text files (e.g., CSV, TSV).
    - Fixed-width formatted files.
    - Custom formats specific to certain applications.
- Rule of thumb:
    - If the file does not follow a specific standard format, use this module
        for reading and writing.
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
    Read DAT content from *path*.

    Parameters
    ----------
    path : Path
        Path to the DAT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the DAT file.
    """
    return stub.read(path, format_name='DAT')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to DAT file at *path* and return record count.

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
    """
    return stub.write(path, data, format_name='DAT')
