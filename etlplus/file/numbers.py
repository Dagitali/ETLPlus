"""
:mod:`etlplus.file.numbers` module.

Stub helpers for reading/writing Apple Numbers (NUMBERS) spreadsheet files (not
implemented yet).

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
    Read NUMBERS content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the NUMBERS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the NUMBERS file.
    """
    return stub.read(path, format_name='NUMBERS')


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to NUMBERS file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the NUMBERS file on disk.
    data : JSONData
        Data to write as NUMBERS file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the NUMBERS file.
    """
    path = coerce_path(path)
    return stub.write(path, data, format_name='NUMBERS')
