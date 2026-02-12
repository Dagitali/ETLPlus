"""
:mod:`etlplus.file.wks` module.

Stub helpers for reading/writing Lotus 1-2-3 (WKS) spreadsheet files (not
implemented yet).

Notes
-----
- A WKS file is a spreadsheet file created using the Lotus 1-2-3 format.
- Common cases:
    - Reading data from legacy Lotus 1-2-3 spreadsheets.
    - Writing data to Lotus 1-2-3 format for compatibility.
    - Converting WKS files to more modern formats.
- Rule of thumb:
    - If you need to work with Lotus 1-2-3 spreadsheet files, use this module
        for reading and writing.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._stub_categories import StubSpreadsheetFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'WksFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class WksFile(StubSpreadsheetFileHandlerABC):
    """
    Stub handler implementation for WKS files.
    """

    # -- Class Attributes -- #

    format = FileFormat.WKS
    engine_name = 'lotus123'

    # -- Instance Methods -- #

    # Inherits read() and write() from StubSpreadsheetFileHandlerABC.


# SECTION: INTERNAL CONSTANTS =============================================== #


_WKS_HANDLER = WksFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``WksFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the WKS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the WKS file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _WKS_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``WksFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the WKS file on disk.
    data : JSONData
        Data to write as WKS file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the WKS file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _WKS_HANDLER.write,
    )
