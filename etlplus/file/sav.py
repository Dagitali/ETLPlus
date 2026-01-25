"""
:mod:`etlplus.file.sav` module.

Helpers for reading/writing SPSS (SAV) data files.

Notes
-----
- A SAV file is a binary file format used by SPSS to store datasets, including
    variables, labels, and data types.
- Common cases:
    - Reading data for analysis in Python.
    - Writing processed data back to SPSS format.
- Rule of thumb:
    - If you need to work with SPSS data files, use this module for reading
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
    Read SAV content from *path*.

    Parameters
    ----------
    path : Path
        Path to the SAV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the SAV file.
    """
    return stub.read(path, format_name='SAV')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to SAV file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the SAV file on disk.
    data : JSONData
        Data to write as SAV file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the SAV file.
    """
    return stub.write(path, data, format_name='SAV')
