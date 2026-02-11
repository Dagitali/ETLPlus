"""
:mod:`etlplus.file.hbs` module.

Stub helpers for reading/writing Handlebars (HBS) template files (not
implemented yet).

Notes
-----
- A Handlebars (HBS) template file is a text file used for generating HTML or
    other text formats by combining templates with data.
- Common cases:
    - HTML templates.
    - Email templates.
    - Configuration files.
- Rule of thumb:
    - If you need to work with Handlebars template files, use this module for
        reading and writing.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import coerce_path
from ._stub_categories import StubTemplateFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'HbsFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class HbsFile(StubTemplateFileHandlerABC):
    """
    Stub handler implementation for HBS files.
    """

    # -- Class Attributes -- #

    format = FileFormat.HBS
    template_engine = 'handlebars'

    # -- Instance Methods -- #

    # Inherits read() and write() from StubTemplateFileHandlerABC.


# SECTION: INTERNAL CONSTANTS =============================================== #


_HBS_HANDLER = HbsFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read and return HBS content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the HBS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the HBS file.
    """
    return _HBS_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to HBS at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the HBS file on disk.
    data : JSONData
        Data to write as HBS file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the HBS file.
    """
    return _HBS_HANDLER.write(coerce_path(path), data)
