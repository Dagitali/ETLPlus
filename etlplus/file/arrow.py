"""
:mod:`etlplus.file.arrow` module.

Helpers for reading/writing Apache Arrow (ARROW) files.

Notes
-----
- An ARROW file is a binary file format designed for efficient
    columnar data storage and processing.
- Common cases:
    - High-performance data analytics.
    - Interoperability between different data processing systems.
    - In-memory data representation for fast computations.
- Rule of thumb:
    - If the file follows the Apache Arrow specification, use this module for
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
    Read ARROW content from *path*.

    Parameters
    ----------
    path : Path
        Path to the Apache Arrow file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the Apache Arrow file.
    """
    return stub.read(path, format_name='ARROW')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to ARROW at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the ARROW file on disk.
    data : JSONData
        Data to write as ARROW. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the ARROW file.
    """
    return stub.write(path, data, format_name='ARROW')
