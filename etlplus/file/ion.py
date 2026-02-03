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
from . import stub
from ._io import coerce_path

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read ION content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the ION file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ION file.
    """
    return stub.read(path, format_name='ION')


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
    path = coerce_path(path)
    return stub.write(path, data, format_name='ION')
