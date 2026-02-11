"""
:mod:`etlplus.file.ion` module.

Stub helpers for reading/writing Amazon Ion (ION) files (not implemented yet).

Notes
-----
- An ION file is a richly-typed, self-describing data format developed by
    Amazon, designed for efficient data interchange and storage.
- Common cases:
    - Data serialization for distributed systems.
    - Interoperability between different programming languages.
    - Handling of complex data types beyond standard JSON capabilities.
- Rule of thumb:
    - If the file follows the Amazon Ion specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import coerce_path
from ._stub_categories import StubSemiStructuredTextFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'IonFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class IonFile(StubSemiStructuredTextFileHandlerABC):
    """
    Stub handler implementation for ION files.
    """

    # -- Class Attributes -- #

    format = FileFormat.ION

    # -- Instance Methods -- #

    # Inherits read() and write() from StubSemiStructuredTextFileHandlerABC.


# SECTION: INTERNAL CONSTANTS =============================================== #


_ION_HANDLER = IonFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read and return ION content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the ION file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ION file.
    """
    return _ION_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to ION at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the ION file on disk.
    data : JSONData
        Data to write as ION. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the ION file.
    """
    return _ION_HANDLER.write(coerce_path(path), data)
