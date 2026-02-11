"""
:mod:`etlplus.file.mdb` module.

Stub helpers for reading/writing newer Microsoft Access database (MDB) files
(not implemented yet).

Notes
-----
- An MDB file is a proprietary database file format used by Microsoft Access
    2003 and earlier.
- Common cases:
    - Storing relational data for small to medium-sized applications.
    - Desktop database applications.
    - Data management for non-enterprise solutions.
- Rule of thumb:
    - If the file follows the MDB specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import coerce_path
from ._stub_categories import StubEmbeddedDatabaseFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MdbFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class MdbFile(StubEmbeddedDatabaseFileHandlerABC):
    """
    Stub handler implementation for MDB files.
    """

    # -- Class Attributes -- #

    format = FileFormat.MDB

    # -- Instance Methods -- #

    # Inherits read() and write() from StubEmbeddedDatabaseFileHandlerABC.


# SECTION: INTERNAL CONSTANTS =============================================== #


_MDB_HANDLER = MdbFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read and return MDB content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the MDB file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the MDB file.
    """
    return _MDB_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to MDB at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the MDB file on disk.
    data : JSONData
        Data to write as MDB. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the MDB file.
    """
    return _MDB_HANDLER.write(coerce_path(path), data)
