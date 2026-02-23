"""
:mod:`etlplus.file.numbers` module.

Stub helpers for reading/writing Apple Numbers (NUMBERS) spreadsheet files (not
implemented yet).

Notes
-----
- A NUMBERS file is a spreadsheet file created by Apple Numbers.
- Common cases:
    - Spreadsheet files created by Apple Numbers.
- Rule of thumb:
    - If you need to read/write NUMBERS files, consider converting them to
        more common formats like CSV or XLSX for better compatibility.
"""

from __future__ import annotations

from ._io import make_deprecated_module_read
from ._io import make_deprecated_module_write
from ._stub_categories import StubSpreadsheetFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'NumbersFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class NumbersFile(StubSpreadsheetFileHandlerABC):
    """
    Stub handler implementation for NUMBERS files.
    """

    # -- Class Attributes -- #

    format = FileFormat.NUMBERS
    engine_name = 'numbers'


# SECTION: INTERNAL CONSTANTS =============================================== #


_NUMBERS_HANDLER = NumbersFile()


# SECTION: FUNCTIONS ======================================================== #


read = make_deprecated_module_read(__name__, _NUMBERS_HANDLER)
write = make_deprecated_module_write(__name__, _NUMBERS_HANDLER)
