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
from ._io import coerce_path
from ._io import warn_deprecated_module_io
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
    Read and return CONF content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the CONF file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the CONF file.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _CONF_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to CONF at *path* and return record count.

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
    warn_deprecated_module_io(__name__, 'write')
    return _CONF_HANDLER.write(coerce_path(path), data)
