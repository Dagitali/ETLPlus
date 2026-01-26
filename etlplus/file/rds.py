"""
:mod:`etlplus.file.rds` module.

Helpers for reading/writing R (RDS) data files.

Notes
-----
- An RDS file is a binary file format used by R to store a single R object,
    such as a data frame, list, or vector.
- Common cases:
    - Storing R objects for later use.
    - Sharing R data between users.
    - Loading R data into Python for analysis.
- Rule of thumb:
    - If the file follows the RDS specification, use this module for reading
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
    Read RDS content from *path*.

    Parameters
    ----------
    path : Path
        Path to the RDS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the RDS file.
    """
    return stub.read(path, format_name='RDS')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to RDS file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the RDS file on disk.
    data : JSONData
        Data to write as RDS file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the RDS file.
    """
    return stub.write(path, data, format_name='RDS')
