"""
:mod:`etlplus.file.psv` module.

Helpers for reading/writing Pipe-Separated Values (PSV) files.

Notes
-----
- A PSV file is a plain text file that uses the pipe character (`|`) to
    separate values.
- Common cases:
    - Each line in the file represents a single record.
    - The first line often contains headers that define the column names.
    - Values may be enclosed in quotes, especially if they contain pipes
        or special characters.
- Rule of thumb:
    - If the file follows the PSV specification, use this module for
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
    'PsvFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class PsvFile(StandardDelimitedTextFileHandlerABC):
    """
    Handler implementation for PSV files.
    """

    # -- Class Attributes -- #

    format = FileFormat.PSV
    delimiter = '|'


# SECTION: INTERNAL CONSTANTS =============================================== #


_PSV_HANDLER = PsvFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``PsvFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the PSV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the PSV file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _PSV_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``PsvFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the PSV file on disk.
    data : JSONData
        Data to write as PSV file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the PSV file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _PSV_HANDLER.write,
    )
