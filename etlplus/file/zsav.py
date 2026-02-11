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
from .base import ReadOptions
from .base import SingleDatasetScientificFileHandlerABC
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


# SECTION: CLASSES ========================================================== #


class ZsavFile(
    SingleDatasetScientificFileHandlerABC,
    stub.StubFileHandlerABC,
):
    """
    Handler implementation for ZSAV files.
    """

    # -- Class Attributes -- #

    format = FileFormat.ZSAV
    dataset_key = 'data'

    # -- Instance Methods -- #

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return one dataset from ZSAV at *path*.
        """
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
        return self.read_dataset(path, options=options)

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
        return self.write_dataset(path, data, options=options)

    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write one dataset to ZSAV at *path* and return record count.
        """
        dataset = self.resolve_write_dataset(dataset, options=options)
        self.validate_single_dataset_key(dataset)
        return super().write(path, data, options=options)


# SECTION: INTERNAL CONSTANTS =============================================== #


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
