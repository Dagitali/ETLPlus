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

from ._enums import FileFormat
from ._stub_categories import StubSemiStructuredTextFileHandlerABC

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ConfFile',
    # Functions
]


# SECTION: CLASSES ========================================================== #


class ConfFile(StubSemiStructuredTextFileHandlerABC):
    """Stub handler implementation for CONF files."""

    # -- Class Attributes -- #

    format = FileFormat.CONF
