"""
:mod:`etlplus.file.cbor` module.

Helpers for reading/writing Concise Binary Object Representation (CBOR) files.

Notes
-----
- A CBOR file is a binary data format designed for small code size and message
    size, suitable for constrained environments.
- Common cases:
    - IoT data interchange.
    - Efficient data serialization.
    - Storage of structured data in a compact binary form.
- Rule of thumb:
    - If the file follows the CBOR specification, use this module for reading
        and writing.
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
    Read CBOR content from *path*.

    Parameters
    ----------
    path : Path
        Path to the CBOR file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the CBOR file.
    """
    return stub.read(path, format_name='CBOR')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to CBOR at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the CBOR file on disk.
    data : JSONData
        Data to write as CBOR. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the CBOR file.
    """
    return stub.write(path, data, format_name='CBOR')
