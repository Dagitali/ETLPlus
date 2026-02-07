"""
:mod:`etlplus.file.accdb` module.

Stub helpers for reading/writing newer Microsoft Access database (ACCDB) files
(not implemented yet).

Notes
-----
- An ACCDB file is a proprietary database file format used by Microsoft Access
    2007 and later.
- Common cases:
    - Storing relational data for small to medium-sized applications.
    - Desktop database applications.
    - Data management for non-enterprise solutions.
- Rule of thumb:
    - If the file follows the ACCDB specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import coerce_path
from .enums import FileFormat
from .stub import StubFileHandlerABC

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AccdbFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class AccdbFile(StubFileHandlerABC):
    """
    Stub handler implementation for ACCDB files.
    """

    # -- Class Attributes -- #

    format = FileFormat.ACCDB


# SECTION: INTERNAL CONSTANTS ============================================== #


_ACCDB_HANDLER = AccdbFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read ACCDB content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the ACCDB file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ACCDB file.
    """
    return _ACCDB_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to ACCDB at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the ACCDB file on disk.
    data : JSONData
        Data to write as ACCDB. Should be a list of dictionaries or a single
        dictionary.

    Returns
    -------
    int
        The number of rows written to the ACCDB file.
    """
    return _ACCDB_HANDLER.write(coerce_path(path), data)
