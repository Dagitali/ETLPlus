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

from ._io import make_deprecated_module_read
from ._io import make_deprecated_module_write
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


# SECTION: INTERNAL CONSTANTS =============================================== #


_ACCDB_HANDLER = AccdbFile()


# SECTION: FUNCTIONS ======================================================== #


read = make_deprecated_module_read(__name__, _ACCDB_HANDLER)
write = make_deprecated_module_write(__name__, _ACCDB_HANDLER)
