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
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._stub_categories import StubEmbeddedDatabaseFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AccdbFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class AccdbFile(StubEmbeddedDatabaseFileHandlerABC):
    """
    Stub handler implementation for ACCDB files.
    """

    # -- Class Attributes -- #

    format = FileFormat.ACCDB

    # -- Instance Methods -- #

    # Inherits read() and write() from StubEmbeddedDatabaseFileHandlerABC.


# SECTION: INTERNAL CONSTANTS =============================================== #


_ACCDB_HANDLER = AccdbFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``AccdbFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the ACCDB file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ACCDB file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _ACCDB_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``AccdbFile().write(...)`` instead.

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
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _ACCDB_HANDLER.write,
    )
