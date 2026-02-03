"""
:mod:`etlplus.file.wks` module.

Stub helpers for reading/writing Lotus 1-2-3 (WKS) spreadsheet files (not
implemented yet).

Notes
-----
- A WKS file is a spreadsheet file created using the Lotus 1-2-3 format.
- Common cases:
    - Reading data from legacy Lotus 1-2-3 spreadsheets.
    - Writing data to Lotus 1-2-3 format for compatibility.
    - Converting WKS files to more modern formats.
- Rule of thumb:
    - If you need to work with Lotus 1-2-3 spreadsheet files, use this module
        for reading and writing.
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
    Read WKS content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the WKS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the WKS file.
    """
    return stub.read(path, format_name='WKS')


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to WKS file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the WKS file on disk.
    data : JSONData
        Data to write as WKS file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the WKS file.
    """
    path = coerce_path(path)
    return stub.write(path, data, format_name='WKS')
