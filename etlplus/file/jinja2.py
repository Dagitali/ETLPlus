"""
:mod:`etlplus.file.jinja2` module.

Stub helpers for reading/writing compressed Jinja2 (JINJA2) template files (not
implemented yet).

Notes
-----
- A JINJA2 file is a text file used for generating HTML or other text formats
    by combining templates with data.
- Common cases:
    - HTML templates.
    - Email templates.
    - Configuration files.
- Rule of thumb:
    - If you need to work with Jinja2 template files, use this module for
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
    'Jinja2File',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class Jinja2File(StubFileHandlerABC):
    """
    Stub handler implementation for JINJA2 files.
    """

    # -- Class Attributes -- #

    format = FileFormat.JINJA2


# SECTION: INTERNAL CONSTANTS ============================================== #


_JINJA2_HANDLER = Jinja2File()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read and return JINJA2 content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the JINJA2 file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the JINJA2 file.
    """
    return _JINJA2_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to JINJA2 at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the JINJA2 file on disk.
    data : JSONData
        Data to write as JINJA2 file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the JINJA2 file.
    """
    return _JINJA2_HANDLER.write(coerce_path(path), data)
