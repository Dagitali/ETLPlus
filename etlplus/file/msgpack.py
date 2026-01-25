"""
:mod:`etlplus.file.msgpack` module.

Helpers for reading/writing MessagePack (MSGPACK) files.

Notes
-----
- A MsgPack file is a binary serialization format that is more compact than
    JSON.
- Common cases:
    - Efficient data storage and transmission.
    - Inter-process communication.
    - Data serialization in performance-critical applications.
- Rule of thumb:
    - If the file follows the MsgPack specification, use this module for
        reading and writing.
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
    Read MsgPack content from *path*.

    Parameters
    ----------
    path : Path
        Path to the MsgPack file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the MsgPack file.
    """
    return stub.read(path, format_name='MSGPACK')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to MsgPack at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the MsgPack file on disk.
    data : JSONData
        Data to write as MsgPack. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the MsgPack file.
    """
    return stub.write(path, data, format_name='MSGPACK')
