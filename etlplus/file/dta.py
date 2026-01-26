"""
:mod:`etlplus.file.dta` module.

Helpers for reading/writing Stata (DTA) data files.

Notes
-----
- Stata DTA files are binary files used by Stata statistical software that
    store datasets with variables, labels, and data types.
- Common cases:
    - Reading data for analysis in Python.
    - Writing processed data back to Stata format.
- Rule of thumb:
    - If you need to work with Stata data files, use this module for reading
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
    Read DTA content from *path*.

    Parameters
    ----------
    path : Path
        Path to the DTA file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the DTA file.
    """
    return stub.read(path, format_name='DTA')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to DTA file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the DTA file on disk.
    data : JSONData
        Data to write as DTA file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the DTA file.
    """
    return stub.write(path, data, format_name='DTA')
