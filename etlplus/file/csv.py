"""
:mod:`etlplus.file.csv` module.

Helpers for reading/writing Comma-Separated Values (CSV) files.

Notes
-----
- A CSV file is a plain text file that uses commas to separate values.
- Common cases:
    - Each line in the file represents a single record.
    - The first line often contains headers that define the column names.
    - Values may be enclosed in quotes, especially if they contain commas
        or special characters.
- Rule of thumb:
    - If the file follows the CSV specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from ._io import make_deprecated_module_read
from ._io import make_deprecated_module_write
from .base import StandardDelimitedTextFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'CsvFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class CsvFile(StandardDelimitedTextFileHandlerABC):
    """
    Handler implementation for CSV files.
    """

    # -- Class Attributes -- #

    format = FileFormat.CSV
    delimiter = ','


# SECTION: INTERNAL CONSTANTS =============================================== #


_CSV_HANDLER = CsvFile()


# SECTION: FUNCTIONS ======================================================== #


read = make_deprecated_module_read(__name__, _CSV_HANDLER)
write = make_deprecated_module_write(__name__, _CSV_HANDLER)
