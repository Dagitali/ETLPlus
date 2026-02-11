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

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import coerce_path
from ._io import warn_deprecated_module_io
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
    dataset_key = 'data'


# SECTION: INTERNAL CONSTANTS =============================================== #


_ZSAV_HANDLER = ZsavFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``ZsavFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the ZSAV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ZSAV file.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _ZSAV_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``ZsavFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the ZSAV file on disk.
    data : JSONData
        Data to write as ZSAV file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the ZSAV file.
    """
    warn_deprecated_module_io(__name__, 'write')
    return _ZSAV_HANDLER.write(coerce_path(path), data)
