"""
:mod:`etlplus.file.xpt` module.

Helpers for reading/writing SAS Transport (XPT) files.

Notes
-----
- A SAS Transport (XPT) file is a standardized file format used to transfer
    SAS datasets between different systems.
- Common cases:
    - Sharing datasets between different SAS installations.
    - Archiving datasets in a platform-independent format.
    - Importing/exporting data to/from statistical software that supports XPT.
- Rule of thumb:
    - If you need to work with XPT files, use this module for reading
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
    Read XPT content from *path*.

    Parameters
    ----------
    path : Path
        Path to the XPT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the XPT file.
    """
    return stub.read(path, format_name='XPT')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to XPT file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the XPT file on disk.
    data : JSONData
        Data to write as XPT file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the XPT file.
    """
    return stub.write(path, data, format_name='XPT')
