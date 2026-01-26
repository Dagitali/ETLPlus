"""
:mod:`etlplus.file.proto` module.

Helpers for reading/writing Protocol Buffers schema (PROTO) files.

Notes
-----
- A PROTO file defines the structure of Protocol Buffers messages.
- Common cases:
    - Defining message formats for data interchange.
    - Generating code for serialization/deserialization.
    - Documenting data structures in distributed systems.
- Rule of thumb:
    - If the file follows the Protocol Buffers schema specification, use this
        module for reading and writing.
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
    Read PROTO content from *path*.

    Parameters
    ----------
    path : Path
        Path to the PROTO file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the PROTO file.
    """
    return stub.read(path, format_name='PROTO')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to PROTO at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the PROTO file on disk.
    data : JSONData
        Data to write as PROTO. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the PROTO file.
    """
    return stub.write(path, data, format_name='PROTO')
