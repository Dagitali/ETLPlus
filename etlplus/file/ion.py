"""
:mod:`etlplus.file.ion` module.

Stub helpers for reading/writing Amazon Ion (ION) files (not implemented yet).

Notes
-----
- An ION file is a richly-typed, self-describing data format developed by
    Amazon, designed for efficient data interchange and storage.
- Common cases:
    - Data serialization for distributed systems.
    - Interoperability between different programming languages.
    - Handling of complex data types beyond standard JSON capabilities.
- Rule of thumb:
    - If the file follows the Amazon Ion specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from ._io import make_deprecated_module_read
from ._io import make_deprecated_module_write
from ._stub_categories import StubSemiStructuredTextFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'IonFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class IonFile(StubSemiStructuredTextFileHandlerABC):
    """
    Stub handler implementation for ION files.
    """

    # -- Class Attributes -- #

    format = FileFormat.ION


# SECTION: INTERNAL CONSTANTS =============================================== #


_ION_HANDLER = IonFile()


# SECTION: FUNCTIONS ======================================================== #


read = make_deprecated_module_read(__name__, _ION_HANDLER)
write = make_deprecated_module_write(__name__, _ION_HANDLER)
