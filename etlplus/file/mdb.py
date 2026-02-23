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

from ._io import make_deprecated_module_io
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


# SECTION: INTERNAL CONSTANTS =============================================== #


_MDB_HANDLER = MdbFile()


# SECTION: FUNCTIONS ======================================================== #


read, write = make_deprecated_module_io(__name__, _MDB_HANDLER)
