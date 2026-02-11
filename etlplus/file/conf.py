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

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
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

    # -- Instance Methods -- #

    # Inherits read() and write() from StubSemiStructuredTextFileHandlerABC.


# SECTION: INTERNAL CONSTANTS =============================================== #


_CONF_HANDLER = ConfFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``ConfFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the CONF file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the CONF file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _CONF_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``ConfFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the CONF file on disk.
    data : JSONData
        Data to write as CONF. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the CONF file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _CONF_HANDLER.write,
    )
