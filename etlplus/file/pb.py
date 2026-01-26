"""
:mod:`etlplus.file.pb` module.

Helpers for reading/writing Protocol Buffer (PB) files.

Notes
-----
- PB (a.k.a. Protobuff) is a binary serialization format developed by Google
    for structured data.
- Common cases:
    - Data interchange between services.
    - Efficient storage of structured data.
    - Communication in distributed systems.
- Rule of thumb:
    - If the file follows the Protocol Buffer specification, use this module
        for reading and writing.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import JSONList
from . import stub

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read PB content from *path*.

    Parameters
    ----------
    path : Path
        Path to the PB file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the PB file.
    """
    return stub.read(path, format_name='PB')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to PB at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the PB file on disk.
    data : JSONData
        Data to write as PB. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the PB file.
    """
    return stub.write(path, data, format_name='PB')
