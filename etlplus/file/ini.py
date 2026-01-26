"""
:mod:`etlplus.file.ini` module.

Helpers for reading/writing initialization (INI) files.

Notes
-----
- An INI file is a simple configuration file format that uses sections,
    properties, and values.
- Common cases:
    - Sections are denoted by square brackets (e.g., ``[section]``).
    - Properties are key-value pairs (e.g., ``key=value``).
    - Comments are often indicated by semicolons (``;``) or hash symbols
        (``#``).
- Rule of thumb:
    - If the file follows the INI specification, use this module for
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
    Read INI content from *path*.

    Parameters
    ----------
    path : Path
        Path to the INI file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the INI file.
    """
    return stub.read(path, format_name='INI')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to INI at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the INI file on disk.
    data : JSONData
        Data to write as INI. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the INI file.
    """
    return stub.write(path, data, format_name='INI')
