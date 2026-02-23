"""
:mod:`etlplus.file.zsav` module.

Stub helpers for reading/writing compressed SPSS (ZSAV) data files (not
implemented yet).

Notes
-----
- A ZSAV file is a compressed binary file format used by SPSS to store
    datasets, including variables, labels, and data types.
- Common cases:
    - Reading compressed data for analysis in Python.
    - Writing processed data back to compressed SPSS format.
- Rule of thumb:
    - If you need to work with compressed SPSS data files, use this module for
        reading and writing.
"""

from __future__ import annotations

from ._io import make_deprecated_module_io
from ._stub_categories import StubSingleDatasetScientificFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ZsavFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class ZsavFile(StubSingleDatasetScientificFileHandlerABC):
    """
    Handler implementation for ZSAV files.
    """

    # -- Class Attributes -- #

    format = FileFormat.ZSAV


# SECTION: INTERNAL CONSTANTS =============================================== #


_ZSAV_HANDLER = ZsavFile()


# SECTION: FUNCTIONS ======================================================== #


read, write = make_deprecated_module_io(__name__, _ZSAV_HANDLER)
