"""
:mod:`etlplus.file.tsv` module.

Helpers for reading/writing Tab-Separated Values (TSV) files.

Notes
-----
- A TSV file is a plain text file that uses the tab character (``\t``) to
    separate values.
- Common cases:
    - Each line in the file represents a single record.
    - The first line often contains headers that define the column names.
    - Values may be enclosed in quotes, especially if they contain tabs
        or special characters.
- Rule of thumb:
    - If the file follows the TSV specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import JSONList
from ._io import read_delimited
from ._io import write_delimited

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
    Read TSV content from *path*.

    Parameters
    ----------
    path : Path
        Path to the TSV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the TSV file.
    """
    return read_delimited(path, delimiter='\t')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to TSV at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the TSV file on disk.
    data : JSONData
        Data to write as TSV. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the TSV file.
    """
    return write_delimited(path, data, delimiter='\t')
