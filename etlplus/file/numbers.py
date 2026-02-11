"""
:mod:`etlplus.file.numbers` module.

Stub helpers for reading/writing Apple Numbers (NUMBERS) spreadsheet files (not
implemented yet).

Notes
-----
- A NUMBERS file is a spreadsheet file created by Apple Numbers.
- Common cases:
    - Spreadsheet files created by Apple Numbers.
- Rule of thumb:
    - If you need to read/write NUMBERS files, consider converting them to
        more common formats like CSV or XLSX for better compatibility.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import coerce_path
from ._io import warn_deprecated_module_io
from ._stub_categories import StubSpreadsheetFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'NumbersFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class NumbersFile(StubSpreadsheetFileHandlerABC):
    """
    Stub handler implementation for NUMBERS files.
    """

    # -- Class Attributes -- #

    format = FileFormat.NUMBERS
    engine_name = 'numbers'

    # -- Instance Methods -- #

    # Inherits read() and write() from StubSpreadsheetFileHandlerABC.


# SECTION: INTERNAL CONSTANTS =============================================== #


_NUMBERS_HANDLER = NumbersFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``NumbersFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the NUMBERS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the NUMBERS file.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _NUMBERS_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``NumbersFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the NUMBERS file on disk.
    data : JSONData
        Data to write as NUMBERS file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the NUMBERS file.
    """
    warn_deprecated_module_io(__name__, 'write')
    return _NUMBERS_HANDLER.write(coerce_path(path), data)
