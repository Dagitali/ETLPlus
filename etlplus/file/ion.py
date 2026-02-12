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
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
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
    Deprecated wrapper. Use ``IonFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the ION file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ION file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _ION_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``IonFile().write(...)`` instead.

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
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _ION_HANDLER.write,
    )
