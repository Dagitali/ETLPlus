"""
:mod:`etlplus.file.fwf` module.

Helpers for reading/writing Fixed-Width Fields (FWF) files.

Notes
-----
- An FWF file is a text file format where each field has a fixed width.
- Common cases:
    - Data files from legacy systems.
    - Reports with aligned columns.
    - Data exchange in mainframe environments.
- Rule of thumb:
    - If the file follows the FWF specification, use this module for
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
    Read FWF content from *path*.

    Parameters
    ----------
    path : Path
        Path to the FWF file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the FWF file.
    """
    return stub.read(path, format_name='FWF')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to FWF file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the FWF file on disk.
    data : JSONData
        Data to write as FWF file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the FWF file.
    """
    return stub.write(path, data, format_name='FWF')
