"""
:mod:`etlplus.file.mat` module.

Helpers for reading/writing MATLAB (MAT) data files.

Notes
-----
- A MAT file is a binary file format used by MATLAB to store variables,
    arrays, and other data structures.
- Common cases:
    - Storing numerical arrays and matrices.
    - Saving workspace variables.
    - Sharing data between MATLAB and other programming environments.
- Rule of thumb:
    - If the file follows the MAT-file specification, use this module for
        reading and writing.
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
    Read MAT content from *path*.

    Parameters
    ----------
    path : Path
        Path to the MAT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the MAT file.
    """
    return stub.read(path, format_name='MAT')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to MAT file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the MAT file on disk.
    data : JSONData
        Data to write as MAT file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the MAT file.
    """
    return stub.write(path, data, format_name='MAT')
