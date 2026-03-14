"""
:mod:`etlplus.file.sylk` module.

Stub helpers for reading/writing Symbolic Link (SYLK) data files (not
implemented yet).

Notes
-----
- A SYLK file is a text-based file format used to represent spreadsheet
    data, including cell values, formulas, and formatting.
- Common cases:
    - Storing spreadsheet data in a human-readable format.
    - Exchanging data between different spreadsheet applications.
- Rule of thumb:
    - If you need to work with SYLK files, use this module for reading
        and writing.
"""

from __future__ import annotations

from ._stub_categories import StubSingleDatasetScientificFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SylkFile',
]


# SECTION: CLASSES ========================================================== #


class SylkFile(StubSingleDatasetScientificFileHandlerABC):
    """Handler implementation for SYLK files."""

    # -- Class Attributes -- #

    format = FileFormat.SYLK
