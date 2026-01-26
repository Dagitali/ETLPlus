"""
:mod:`etlplus.file.ion` module.

Helpers for reading/writing Amazon Ion (ION) files.

Notes
-----
- An ION file is a richly-typed, self-describing data format developed by
    Amazon, designed for efficient data interchange and storage.
- Common cases:
    - Data serialization for distributed systems.
    - Interoperability between different programming languages.
    - Handling of complex data types beyond standard JSON capabilities.
- Rule of thumb:
    - If the file follows the Amazon Ion specification, use this module for
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
    Read ION content from *path*.

    Parameters
    ----------
    path : Path
        Path to the ION file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ION file.
    """
    return stub.read(path, format_name='ION')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to ION at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the ION file on disk.
    data : JSONData
        Data to write as ION. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the ION file.
    """
    return stub.write(path, data, format_name='ION')
