"""
:mod:`etlplus.file.bson` module.

Helpers for reading/writing Binary JSON (BSON) files.

Notes
-----
- A BSON file is a binary-encoded serialization of JSON-like documents.
- Common cases:
    - Data storage in MongoDB.
    - Efficient data interchange between systems.
    - Handling of complex data types not supported in standard JSON.
- Rule of thumb:
    - If the file follows the BSON specification, use this module for reading
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
    Read BSON content from *path*.

    Parameters
    ----------
    path : Path
        Path to the BSON file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the BSON file.
    """
    return stub.read(path, format_name='BSON')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to BSON at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the BSON file on disk.
    data : JSONData
        Data to write as BSON. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the BSON file.
    """
    return stub.write(path, data, format_name='BSON')
