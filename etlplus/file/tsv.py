r"""
:mod:`etlplus.file.tsv` module.

Helpers for reading/writing Tab-Separated Values (TSV) files.

Notes
-----
- A TSV file is a plain text file that uses the tab character (``\\t``) to
    separate values.
- Common cases:
    - Each line in the file represents a single record.
    - The first line often contains headers that define the column names.
    - Values may be enclosed in quotes, especially if they contain tabs
        or special characters.
- Rule of thumb:
    - If the file follows the TSV specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from .base import StandardDelimitedTextFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'TsvFile',
]


# SECTION: CLASSES ========================================================== #


class TsvFile(StandardDelimitedTextFileHandlerABC):
    """Handler implementation for TSV files."""

    # -- Class Attributes -- #

    format = FileFormat.TSV
    delimiter = '\t'
