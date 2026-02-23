"""
:mod:`etlplus.file.tab` module.

Helpers for reading/writing "tab"-formatted (TAB) files.

Notes
-----
- A TAB file is not necessarily a TSV file when tabs aren’t actually the
    delimiter that defines the fields, even if the text looks column-aligned.
- Common cases:
    - Fixed-width text (FWF) that uses tabs for alignment.
    - Mixed whitespace (tabs + spaces) as “pretty printing”.
    - Tabs embedded inside quoted fields (or unescaped tabs in free text).
    - Header/metadata lines or multi-line records that break TSV assumptions.
    - Not actually tab-delimited despite the name.
- Rule of thumb:
    - This implementation treats TAB as tab-delimited text.
    - If the file has fixed-width fields, use :mod:`etlplus.file.fwf`.
"""

from __future__ import annotations

from ._io import make_deprecated_module_read
from ._io import make_deprecated_module_write
from .base import StandardDelimitedTextFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'TabFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class TabFile(StandardDelimitedTextFileHandlerABC):
    """
    Handler implementation for TAB files.
    """

    # -- Class Attributes -- #

    format = FileFormat.TAB
    delimiter = '\t'


# SECTION: INTERNAL CONSTANTS =============================================== #


_TAB_HANDLER = TabFile()


# SECTION: FUNCTIONS ======================================================== #


read = make_deprecated_module_read(__name__, _TAB_HANDLER)
write = make_deprecated_module_write(__name__, _TAB_HANDLER)
