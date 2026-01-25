"""
:mod:`etlplus.file.xlsm` module.

Helpers for reading/writing Microsoft Excel Macro-Enabled (XLSM) spreadsheet
files.

Notes
-----
- An XLSM file is a spreadsheet file created using the Microsoft Excel Macro-
    Enabled (Open XML) format.
- Common cases:
    - Reading data from Excel Macro-Enabled spreadsheets.
    - Writing data to Excel Macro-Enabled format for compatibility.
    - Converting XLSM files to more modern formats.
- Rule of thumb:
    - If you need to work with Excel Macro-Enabled spreadsheet files, use this
        module for reading and writing.
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
    Read XLSM content from *path*.

    Parameters
    ----------
    path : Path
        Path to the XLSM file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the XLSM file.
    """
    return stub.read(path, format_name='XLSM')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to XLSM file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the XLSM file on disk.
    data : JSONData
        Data to write as XLSM file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the XLSM file.
    """
    return stub.write(path, data, format_name='XLSM')
