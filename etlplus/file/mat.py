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
from .base import ReadOptions
from .base import SingleDatasetScientificFileHandlerABC
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


# SECTION: CLASSES ========================================================== #


class MatFile(
    SingleDatasetScientificFileHandlerABC,
    stub.StubFileHandlerABC,
):
    """
    Handler implementation for MAT files.
    """

    # -- Class Attributes -- #

    format = FileFormat.MAT
    dataset_key = 'data'

    # -- Instance Methods -- #

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """Read and return one dataset from MAT at *path*."""
        dataset = self.resolve_read_dataset(dataset, options=options)
        self.validate_single_dataset_key(dataset)
        return super().read(path, options=options)

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
        return self.read_dataset(path, options=options)

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
        return self.write_dataset(path, data, options=options)

    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:
        """Write one dataset to MAT at *path* and return record count."""
        dataset = self.resolve_write_dataset(dataset, options=options)
        self.validate_single_dataset_key(dataset)
        return super().write(path, data, options=options)


# SECTION: INTERNAL CONSTANTS =============================================== #


_MAT_HANDLER = MatFile()


# SECTION: FUNCTIONS ======================================================== #


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
