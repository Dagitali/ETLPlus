"""
:mod:`etlplus.file.mat` module.

Stub helpers for reading/writing MATLAB (MAT) data files (not implemented yet).

Notes
-----
- A MAT file is a binary file format used by MATLAB to store variables,
    arrays, and other data structures.
- Common cases:
    - Storing numerical arrays and matrices.
    - Saving workspace variables.
    - Sharing data between MATLAB and other programming environments.
- Rule of thumb:
    - If the file follows the MAT-file specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from ._enums import FileFormat
from ._stub_categories import StubSingleDatasetScientificFileHandlerABC

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MatFile',
]


# SECTION: CLASSES ========================================================== #


class MatFile(StubSingleDatasetScientificFileHandlerABC):
    """Handler implementation for MAT files."""

    # -- Class Attributes -- #

    format = FileFormat.MAT
