"""
:mod:`etlplus.file.sas7bdat` module.

Helpers for reading/writing SAS (SAS7BDAT) data files.

Notes
-----
- A SAS7BDAT file is a binary file format used by SAS to store datasets,
    including variables, labels, and data types.
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
        Path to the SAS7BDAT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the SAS7BDAT file.
    """
    return stub.read(path, format_name='SAS7BDAT')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to SAS7BDAT file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the SAS7BDAT file on disk.
    data : JSONData
        Data to write as SAS7BDAT file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the SAS7BDAT file.
    """
    return stub.write(path, data, format_name='SAS7BDAT')
