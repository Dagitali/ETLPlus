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

from pathlib import Path

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from . import stub
from ._io import coerce_path
from .base import FileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ZsavFile',
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


class ZsavFile(FileHandlerABC):
    """
    Handler implementation for ZSAV files.
    """

    format = FileFormat.ZSAV
    category = 'statistical_dataset'

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read ZSAV content from *path*.

        Parameters
        ----------
        path : Path
            Path to the ZSAV file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the ZSAV file.
        """
        _ = options
        return stub.read(path, format_name='ZSAV')

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to ZSAV file at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the ZSAV file on disk.
        data : JSONData
            Data to write as ZSAV file. Should be a list of dictionaries or a
            single dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the ZSAV file.
        """
        _ = options
        return stub.write(path, data, format_name='ZSAV')


# SECTION: INTERNAL CONSTANTS ============================================== #


_ZSAV_HANDLER = ZsavFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read ZSAV content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the ZSAV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ZSAV file.
    """
    return _ZSAV_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to ZSAV file at *path* and return record count.

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
    return _ZSAV_HANDLER.write(coerce_path(path), data)
