"""
:mod:`etlplus.file.sylk` module.

Stub helpers for reading/writing Symbolic Link (SYLK) data files (not
implemented yet).

Notes
-----
- A SYLK file is a text-based file format used to represent spreadsheet
    data, including cell values, formulas, and formatting.
- Common cases:
    - Storing spreadsheet data in a human-readable format.
    - Exchanging data between different spreadsheet applications.
- Rule of thumb:
    - If you need to work with SYLK files, use this module for reading
        and writing.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import coerce_path
from ._io import warn_deprecated_module_io
from ._stub_categories import StubSingleDatasetScientificFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SylkFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class SylkFile(StubSingleDatasetScientificFileHandlerABC):
    """
    Handler implementation for SYLK files.
    """

    # -- Class Attributes -- #

    format = FileFormat.SYLK
    dataset_key = 'data'


# SECTION: INTERNAL CONSTANTS =============================================== #


_SYLK_HANDLER = SylkFile()


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``SylkFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the SYLK file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the SYLK file.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _SYLK_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``SylkFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the SYLK file on disk.
    data : JSONData
        Data to write as SYLK file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the SYLK file.
    """
    warn_deprecated_module_io(__name__, 'write')
    return _SYLK_HANDLER.write(coerce_path(path), data)
