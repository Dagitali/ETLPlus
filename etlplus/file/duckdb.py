"""
:mod:`etlplus.file.duckdb` module.

Helpers for reading/writing DuckDB database (DUCKDB) files.

Notes
-----
- A DUCKDB file is a self-contained, serverless database file format used by
    DuckDB.
- Common cases:
    - Analytical data storage and processing.
    - Embedded database applications.
    - Fast querying of large datasets.
- Rule of thumb:
    - If the file follows the DUCKDB specification, use this module for reading
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
    Read DUCKDB content from *path*.

    Parameters
    ----------
    path : Path
        Path to the DUCKDB file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the DUCKDB file.
    """
    return stub.read(path, format_name='DUCKDB')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to DUCKDB at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the DUCKDB file on disk.
    data : JSONData
        Data to write as DUCKDB. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the DUCKDB file.
    """
    return stub.write(path, data, format_name='DUCKDB')
