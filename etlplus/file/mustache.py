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
from ._io import coerce_path
from .enums import FileFormat
from .stub import StubFileHandlerABC

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MustacheFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class MustacheFile(StubFileHandlerABC):
    """
    Stub handler implementation for MUSTACHE files.
    """

    # -- Class Attributes -- #

    format = FileFormat.MUSTACHE


# SECTION: INTERNAL CONSTANTS ============================================== #


_MUSTACHE_HANDLER = MustacheFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read and return MUSTACHE content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the MUSTACHE file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the MUSTACHE file.
    """
    return _MUSTACHE_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to MUSTACHE at *path* and return record count.

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
    return _MUSTACHE_HANDLER.write(coerce_path(path), data)
