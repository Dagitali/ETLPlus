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

from ._enums import FileFormat
from ._stub_categories import StubEmbeddedDatabaseFileHandlerABC

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AccdbFile',
]


# SECTION: CLASSES ========================================================== #


class AccdbFile(StubEmbeddedDatabaseFileHandlerABC):
    """Stub handler implementation for ACCDB files."""

    # -- Class Attributes -- #

    format = FileFormat.ACCDB
