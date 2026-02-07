"""
:mod:`etlplus.file.sylk` module.

Stub helpers for reading/writing Symbolic Link (SYLK) data files (not
implemented yet).

Notes
-----
- A SYLK file is a text-based file format used to represent spreadsheet
    data, including cell values, formulas, and formatting.
- Common cases:
    - Storing spreadsheet data in a human-readable format.
    - Exchanging data between different spreadsheet applications.
- Rule of thumb:
    - If you need to work with SYLK files, use this module for reading
        and writing.
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
    'SylkFile',
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


class SylkFile(ScientificDatasetFileHandlerABC):
    """
    Handler implementation for SYLK files.
    """

    format = FileFormat.SYLK
    dataset_key = 'data'

    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:
        """
        Return available SYLK dataset keys.
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
        Read one dataset from SYLK at *path*.
        """
        _ = options
        if dataset is not None and dataset != self.dataset_key:
            raise ValueError(
                f'SYLK supports only dataset key {self.dataset_key!r}',
            )
        return stub.read(path, format_name='SYLK')

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read SYLK content from *path*.

        Parameters
        ----------
        path : Path
            Path to the SYLK file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the SYLK file.
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
        Write *data* to SYLK file at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the SYLK file on disk.
        data : JSONData
            Data to write as SYLK file. Should be a list of dictionaries or a
            single dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the SYLK file.
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
        Write one dataset to SYLK at *path*.
        """
        _ = options
        if dataset is not None and dataset != self.dataset_key:
            raise ValueError(
                f'SYLK supports only dataset key {self.dataset_key!r}',
            )
        return stub.write(path, data, format_name='SYLK')


# SECTION: INTERNAL CONSTANTS ============================================== #


_SYLK_HANDLER = SylkFile()


def read(
    path: StrPath,
) -> JSONList:
    """
    Read SYLK content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the SYLK file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the SYLK file.
    """
    return _SYLK_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to SYLK file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the SYLK file on disk.
    data : JSONData
        Data to write as SYLK file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the SYLK file.
    """
    return _SYLK_HANDLER.write(coerce_path(path), data)
