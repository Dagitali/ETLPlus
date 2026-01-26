"""
:mod:`etlplus.file.zsav` module.

Helpers for reading/writing compressed SPSS (ZSAV) data files.

Notes
-----
- A ZSAV file is a compressed binary file format used by SPSS to store
    datasets, including variables, labels, and data types.
- Common cases:
    - Reading compressed data for analysis in Python.
    - Writing processed data back to compressed SPSS format.
- Rule of thumb:
    - If you need to work with compressed SPSS data files, use this module for
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
    Read ZSAV content from *path*.

    Parameters
    ----------
    path : Path
        Path to the ZSAV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ZSAV file.
    """
    return stub.read(path, format_name='ZSAV')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to ZSAV file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the ZSAV file on disk.
    data : JSONData
        Data to write as ZSAV file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the ZSAV file.
    """
    return stub.write(path, data, format_name='ZSAV')
