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
from ._io import warn_deprecated_module_io
from ._stub_categories import StubTemplateFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'Jinja2File',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class Jinja2File(StubTemplateFileHandlerABC):
    """
    Stub handler implementation for JINJA2 files.
    """

    # -- Class Attributes -- #

    format = FileFormat.JINJA2
    template_engine = 'jinja2'

    # -- Instance Methods -- #

    # Inherits read() and write() from StubTemplateFileHandlerABC.


# SECTION: INTERNAL CONSTANTS =============================================== #


_JINJA2_HANDLER = Jinja2File()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``Jinja2File().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the JINJA2 file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the JINJA2 file.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _JINJA2_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``Jinja2File().write(...)`` instead.

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
    warn_deprecated_module_io(__name__, 'write')
    return _JINJA2_HANDLER.write(coerce_path(path), data)
