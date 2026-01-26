"""
:mod:`etlplus.file.pbf` module.

Helpers for reading/writing Protocolbuffer Binary Format (PBF) files.

Notes
-----
- PBF is a binary format used primarily for OpenStreetMap (OSM) data.
- Common cases:
    - Efficient storage of large OSM datasets.
    - Fast data interchange for mapping applications.
    - Compression of OSM data for reduced file size.
- Rule of thumb:
    - If the file follows the PBF specification, use this module for reading
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
    Read PBF content from *path*.

    Parameters
    ----------
    path : Path
        Path to the PBF file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the PBF file.
    """
    return stub.read(path, format_name='PBF')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to PBF at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the PBF file on disk.
    data : JSONData
        Data to write as PBF. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the PBF file.
    """
    return stub.write(path, data, format_name='PBF')
