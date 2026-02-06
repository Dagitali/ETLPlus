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
    'MatFile',
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


class MatFile(FileHandlerABC):
    """
    Handler implementation for MAT files.
    """

    format = FileFormat.MAT
    category = 'scientific_dataset'

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read MAT content from *path*.

        Parameters
        ----------
        path : Path
            Path to the MAT file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the MAT file.
        """
        _ = options
        return stub.read(path, format_name='MAT')

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to MAT file at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the MAT file on disk.
        data : JSONData
            Data to write as MAT file. Should be a list of dictionaries or a
            single dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the MAT file.
        """
        _ = options
        return stub.write(path, data, format_name='MAT')


# SECTION: INTERNAL CONSTANTS ============================================== #


_MAT_HANDLER = MatFile()


def read(
    path: StrPath,
) -> JSONList:
    """
    Read MAT content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the MAT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the MAT file.
    """
    return _MAT_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to MAT file at *path* and return record count.

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
    return _MAT_HANDLER.write(coerce_path(path), data)
