"""
:mod:`etlplus.file.wks` module.

Stub helpers for reading/writing Lotus 1-2-3 (WKS) spreadsheet files (not
implemented yet).

Notes
-----
- A WKS file is a spreadsheet file created using the Lotus 1-2-3 format.
- Common cases:
    - Reading data from legacy Lotus 1-2-3 spreadsheets.
    - Writing data to Lotus 1-2-3 format for compatibility.
    - Converting WKS files to more modern formats.
- Rule of thumb:
    - If you need to work with Lotus 1-2-3 spreadsheet files, use this module
        for reading and writing.
"""

from __future__ import annotations

from ._enums import FileFormat
from ._stub_categories import StubSpreadsheetFileHandlerABC

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'WksFile',
]


# SECTION: CLASSES ========================================================== #


class WksFile(StubSpreadsheetFileHandlerABC):
    """Stub handler implementation for WKS files."""

    # -- Class Attributes -- #

    format = FileFormat.WKS
    engine_name = 'lotus123'
