"""
:mod:`etlplus.file.cfg` module.

Stub helpers for reading/writing config (CFG) files (not implemented yet).

Notes
-----
- A CFG file is a configuration file that may use various syntaxes, such as
    INI, YAML, or custom formats.
- Common cases:
    - INI-style key-value pairs with sections (such as in Python ecosystems,
        using ``configparser``).
    - YAML-like structures with indentation.
    - Custom formats specific to certain applications.
- Rule of thumb:
    - If the file follows a standard format like INI or YAML, consider using
        dedicated parsers.
"""

from __future__ import annotations

from ._io import make_deprecated_module_read
from ._io import make_deprecated_module_write
from ._stub_categories import StubSemiStructuredTextFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'CfgFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class CfgFile(StubSemiStructuredTextFileHandlerABC):
    """
    Stub handler implementation for CFG files.
    """

    # -- Class Attributes -- #

    format = FileFormat.CFG


# SECTION: INTERNAL CONSTANTS =============================================== #


_CFG_HANDLER = CfgFile()


# SECTION: FUNCTIONS ======================================================== #


read = make_deprecated_module_read(__name__, _CFG_HANDLER)
write = make_deprecated_module_write(__name__, _CFG_HANDLER)
