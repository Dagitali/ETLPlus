"""
:mod:`etlplus.file.csv` module.

Helpers for reading/writing Comma-Separated Values (CSV) files.

Notes
-----
- A CSV file is a plain text file that uses commas to separate values.
- Common cases:
    - Each line in the file represents a single record.
    - The first line often contains headers that define the column names.
    - Values may be enclosed in quotes, especially if they contain commas
        or special characters.
- Rule of thumb:
    - If the file follows the CSV specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from .base import StandardDelimitedTextFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'CsvFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class CsvFile(StandardDelimitedTextFileHandlerABC):
    """
    Handler implementation for CSV files.
    """

    # -- Class Attributes -- #

    format = FileFormat.CSV
    delimiter = ','


# SECTION: INTERNAL CONSTANTS =============================================== #


_CSV_HANDLER = CsvFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``CsvFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the CSV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the CSV file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _CSV_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``CsvFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the CSV file on disk.
    data : JSONData
        Data to write as CSV. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the CSV file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _CSV_HANDLER.write,
    )
