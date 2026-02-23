"""
:mod:`etlplus.file.conf` module.

Stub helpers for reading/writing config (CONF) files (not implemented yet).

Notes
-----
- A CONF file is a configuration file that may use various syntaxes, such as
    INI, YAML, or custom formats.
- Common cases:
    - INI-style key-value pairs with sections.
    - YAML-like structures with indentation.
    - Custom formats specific to certain applications (such as Unix-like
        systems, where ``.conf`` is a strong convention for "This is a
        configuration file").
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
    'ConfFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class ConfFile(StubSemiStructuredTextFileHandlerABC):
    """
    Stub handler implementation for CONF files.
    """

    # -- Class Attributes -- #

    format = FileFormat.CONF


# SECTION: INTERNAL CONSTANTS =============================================== #


_CONF_HANDLER = ConfFile()


# SECTION: FUNCTIONS ======================================================== #


read = make_deprecated_module_read(__name__, _CONF_HANDLER)
write = make_deprecated_module_write(__name__, _CONF_HANDLER)
