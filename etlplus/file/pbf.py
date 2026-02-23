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

from ._stub_categories import StubBinarySerializationFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PbfFile',
]


# SECTION: CLASSES ========================================================== #


class PbfFile(StubBinarySerializationFileHandlerABC):
    """
    Stub handler implementation for PBF files.
    """

    # -- Class Attributes -- #

    format = FileFormat.PBF
