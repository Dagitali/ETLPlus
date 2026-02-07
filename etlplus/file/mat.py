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
from .base import ScientificDatasetFileHandlerABC
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


class MatFile(ScientificDatasetFileHandlerABC):
    """
    Handler implementation for MAT files.
    """

    # -- Class Attributes -- #

    format = FileFormat.MAT
    dataset_key = 'data'

    # -- Instance Methods -- #

    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:
        """
        Return available MAT dataset keys.
        """
        _ = path
        return [self.dataset_key]

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read one dataset from MAT at *path*.
        """
        _ = options
        if dataset is not None and dataset != self.dataset_key:
            raise ValueError(
                f'MAT supports only dataset key {self.dataset_key!r}',
            )
        return stub.read(path, format_name='MAT')

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
        dataset = options.dataset if options is not None else None
        return self.read_dataset(path, dataset=dataset, options=options)

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
        dataset = options.dataset if options is not None else None
        return self.write_dataset(
            path,
            data,
            dataset=dataset,
            options=options,
        )

    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write one dataset to MAT at *path*.
        """
        _ = options
        if dataset is not None and dataset != self.dataset_key:
            raise ValueError(
                f'MAT supports only dataset key {self.dataset_key!r}',
            )
        return stub.write(path, data, format_name='MAT')


# SECTION: INTERNAL CONSTANTS ============================================== #


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
