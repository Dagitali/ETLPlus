"""
:mod:`etlplus.file.csv` module.

Helpers for reading/writing CSV files.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import JSONList
from ._io import read_delimited
from ._io import write_delimited

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read CSV content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the CSV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the CSV file.
    """
    return read_delimited(path, delimiter=',')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write ``data`` to CSV at ``path`` and return record count.

    Parameters
    ----------
    path : Path
        Path to the CSV file on disk.
    data : JSONData
        Data to write as CSV. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the CSV file.
    """
    return write_delimited(path, data, delimiter=',')
