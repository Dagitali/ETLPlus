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

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._stub_categories import StubSingleDatasetScientificFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MatFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class MatFile(StubSingleDatasetScientificFileHandlerABC):
    """
    Handler implementation for MAT files.
    """

    # -- Class Attributes -- #

    format = FileFormat.MAT
    dataset_key = 'data'


# SECTION: INTERNAL CONSTANTS =============================================== #


_MAT_HANDLER = MatFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``MatFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the MAT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the MAT file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _MAT_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``MatFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the MAT file on disk.
    data : JSONData
        Data to write as MAT file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the MAT file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _MAT_HANDLER.write,
    )
