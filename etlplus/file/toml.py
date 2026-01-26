"""
:mod:`etlplus.file.toml` module.

Helpers for reading/writing Tom's Obvious Minimal Language (TOML) files.

Notes
-----
- A TOML file is a configuration file that uses the TOML syntax.
- Common cases:
    - Simple key-value pairs.
    - Nested tables and arrays.
    - Data types such as strings, integers, floats, booleans, dates, and
        arrays.
- Rule of thumb:
    - If the file follows the TOML specification, use this module for
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
    Read TOML content from *path*.

    Parameters
    ----------
    path : Path
        Path to the TOML file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the TOML file.
    """
    return stub.read(path, format_name='TOML')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to TOML at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the TOML file on disk.
    data : JSONData
        Data to write as TOML. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the TOML file.
    """
    return stub.write(path, data, format_name='TOML')
