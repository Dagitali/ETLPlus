"""
:mod:`etlplus.file.psv` module.

Helpers for reading/writing Pipe-Separated Values (PSV) files.

Notes
-----
- A PSV file is a plain text file that uses the pipe character (`|`) to
    separate values.
- Common cases:
    - Each line in the file represents a single record.
    - The first line often contains headers that define the column names.
    - Values may be enclosed in quotes, especially if they contain pipes
        or special characters.
- Rule of thumb:
    - If the file follows the PSV specification, use this module for
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
    Read PSV content from *path*.

    Parameters
    ----------
    path : Path
        Path to the PSV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the PSV file.
    """
    return stub.read(path, format_name='PSV')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to PSV file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the PSV file on disk.
    data : JSONData
        Data to write as PSV file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the PSV file.
    """
    return stub.write(path, data, format_name='PSV')
