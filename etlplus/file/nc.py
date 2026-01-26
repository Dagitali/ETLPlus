"""
:mod:`etlplus.file.nc` module.

Helpers for reading/writing NetCDF (NC) data files.

Notes
-----
- A NC file is a binary file format used for array-oriented scientific data,
    particularly in meteorology, oceanography, and climate science.
- Common cases:
    - Storing multi-dimensional scientific data.
    - Sharing large datasets in research communities.
    - Efficient data access and manipulation.
- Rule of thumb:
    - If the file follows the NetCDF standard, use this module for
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
    Read NC content from *path*.

    Parameters
    ----------
    path : Path
        Path to the NC file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the NC file.
    """
    return stub.read(path, format_name='NC')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to NC file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the NC file on disk.
    data : JSONData
        Data to write as NC file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the NC file.
    """
    return stub.write(path, data, format_name='NC')
