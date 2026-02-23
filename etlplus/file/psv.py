"""
:mod:`etlplus.file.psv` module.

Helpers for reading/writing Pipe-Separated Values (PSV) files.

Notes
-----
- A PSV file is a plain text file that uses the pipe character (`|`) to
    separate values.
- Common cases:
    - Each line in the file represents a single record.
    - The first line often contains headers that define the column names.
    - Values may be enclosed in quotes, especially if they contain pipes
        or special characters.
- Rule of thumb:
    - If the file follows the PSV specification, use this module for
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
    'PsvFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class PsvFile(StandardDelimitedTextFileHandlerABC):
    """
    Handler implementation for PSV files.
    """

    # -- Class Attributes -- #

    format = FileFormat.PSV
    delimiter = '|'


# SECTION: INTERNAL CONSTANTS =============================================== #


_PSV_HANDLER = PsvFile()


# SECTION: FUNCTIONS ======================================================== #


read = make_deprecated_module_read(__name__, _PSV_HANDLER)
write = make_deprecated_module_write(__name__, _PSV_HANDLER)
