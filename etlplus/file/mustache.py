"""
:mod:`etlplus.file.mustache` module.

Stub helpers for reading/writing Mustache (MUSTACHE) template files (not
implemented yet).

Notes
-----
- A MUSTACHE file is a text file used for generating HTML or other text formats
    by combining templates with data.
- Common cases:
    - HTML templates.
    - Email templates.
    - Configuration files.
- Rule of thumb:
    - If you need to work with Mustache template files, use this module for
        reading and writing.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._stub_categories import StubTemplateFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MustacheFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class MustacheFile(StubTemplateFileHandlerABC):
    """
    Stub handler implementation for MUSTACHE files.
    """

    # -- Class Attributes -- #

    format = FileFormat.MUSTACHE
    template_engine = 'mustache'

    # -- Instance Methods -- #

    # Inherits read() and write() from StubTemplateFileHandlerABC.


# SECTION: INTERNAL CONSTANTS =============================================== #


_MUSTACHE_HANDLER = MustacheFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``MustacheFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the MUSTACHE file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the MUSTACHE file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _MUSTACHE_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``MustacheFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the MUSTACHE file on disk.
    data : JSONData
        Data to write as MUSTACHE file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the MUSTACHE file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _MUSTACHE_HANDLER.write,
    )
