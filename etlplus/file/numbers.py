"""
:mod:`etlplus.file.numbers` module.

Helpers for reading/writing Apple Numbers (NUMBERS) spreadsheet files.

Notes
-----
- A NUMBERS file is a spreadsheet file created by Apple Numbers.
- Common cases:
    - Spreadsheet files created by Apple Numbers.
- Rule of thumb:
    - If you need to read/write NUMBERS files, consider converting them to
        more common formats like CSV or XLSX for better compatibility.
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
    Read NUMBERS content from *path*.

    Parameters
    ----------
    path : Path
        Path to the NUMBERS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the NUMBERS file.
    """
    return stub.read(path, format_name='NUMBERS')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to NUMBERS file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the NUMBERS file on disk.
    data : JSONData
        Data to write as NUMBERS file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the NUMBERS file.
    """
    return stub.write(path, data, format_name='NUMBERS')
