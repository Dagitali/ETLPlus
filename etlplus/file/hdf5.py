"""
:mod:`etlplus.file.hdf5` module.

Helpers for reading/writing Hierarchical Data Format (HDF5) files.

Notes
-----
- A HDF5 file is a binary file format designed to store and organize large
    amounts of data.
- Common cases:
    - Scientific data storage and sharing.
    - Large-scale data analysis.
    - Hierarchical data organization.
- Rule of thumb:
    - If the file follows the HDF5 specification, use this module for reading
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
    Read HDF5 content from *path*.

    Parameters
    ----------
    path : Path
        Path to the HDF5 file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the HDF5 file.
    """
    return stub.read(path, format_name='HDF5')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to HDF5 file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the HDF5 file on disk.
    data : JSONData
        Data to write as HDF5 file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the HDF5 file.
    """
    return stub.write(path, data, format_name='HDF5')
