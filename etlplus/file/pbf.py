"""
:mod:`etlplus.file.pbf` module.

Stub helpers for reading/writing Protocolbuffer Binary Format (PBF) files (not
implemented yet).

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

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._stub_categories import StubBinarySerializationFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PbfFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class PbfFile(StubBinarySerializationFileHandlerABC):
    """
    Stub handler implementation for PBF files.
    """

    # -- Class Attributes -- #

    format = FileFormat.PBF

    # -- Instance Methods -- #

    # Inherits read() and write() from StubBinarySerializationFileHandlerABC.


# SECTION: INTERNAL CONSTANTS =============================================== #


_PBF_HANDLER = PbfFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``PbfFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the PBF file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the PBF file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _PBF_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``PbfFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the PBF file on disk.
    data : JSONData
        Data to write as PBF. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the PBF file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _PBF_HANDLER.write,
    )
