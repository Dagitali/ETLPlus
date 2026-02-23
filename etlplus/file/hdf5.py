"""
:mod:`etlplus.file.hdf5` module.

Helpers for reading Hierarchical Data Format (HDF5) files. Stub helpers for
writing such files (not implemented yet).

Notes
-----
- A HDF5 file is a binary file format designed to store and organize large
    amounts of data.
- Common cases:
    - Scientific data storage and sharing.
    - Large-scale data analysis.
    - Hierarchical data organization.
- Rule of thumb:
    - If the file follows the HDF5 specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..types import JSONData
from ..types import JSONList
from ._imports import get_pandas
from ._io import records_from_table
from .base import ReadOnlyFileHandlerABC
from .base import ReadOptions
from .base import ScientificDatasetFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'Hdf5File',
]


# SECTION: CONSTANTS ======================================================== #


DEFAULT_KEY = 'data'


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _raise_tables_error(
    err: ImportError,
) -> None:
    """
    Raise a consistent ImportError for missing PyTables support.

    Parameters
    ----------
    err : ImportError
        The original ImportError raised when trying to use HDF5 support without
        the required dependency.

    Raises
    ------
    ImportError
        Consistent ImportError indicating that PyTables is required.
    """
    raise ImportError(
        'HDF5 support requires optional dependency "tables".\n'
        'Install with: pip install tables',
    ) from err


# SECTION: CLASSES ========================================================== #


class Hdf5File(ReadOnlyFileHandlerABC, ScientificDatasetFileHandlerABC):
    """
    Read-only handler implementation for HDF5 files.
    """

    # -- Class Attributes -- #

    format = FileFormat.HDF5
    dataset_key = DEFAULT_KEY

    # -- Instance Methods -- #

    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:
        """
        Return available HDF5 dataset keys.

        Parameters
        ----------
        path : Path
            Path to the HDF5 file on disk.

        Returns
        -------
        list[str]
            Dataset keys in the HDF5 store.
        """
        with self.open_store(path) as store:
            return [key.lstrip('/') for key in store.keys()]

    def open_store(
        self,
        path: Path,
    ) -> Any:
        """
        Open and return one HDFStore, wrapping missing tables dependency.
        """
        pandas = self.resolve_pandas()
        try:
            return pandas.HDFStore(path)
        except ImportError as err:  # pragma: no cover
            _raise_tables_error(err)

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return one dataset from HDF5 at *path*.

        Parameters
        ----------
        path : Path
            Path to the HDF5 file on disk.
        dataset : str | None, optional
            Dataset key selector.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            Parsed records.

        Raises
        ------
        ValueError
            If the selected dataset key is missing or ambiguous.
        """
        dataset = self.resolve_dataset(dataset, options=options)
        with self.open_store(path) as store:
            keys = [key.lstrip('/') for key in store.keys()]
            key = self.resolve_store_key(keys, dataset=dataset)
            if key is None:
                return []
            frame = store.get(key)
        return records_from_table(frame)

    def resolve_pandas(self) -> Any:
        """
        Return pandas using module-level dependency resolution.
        """
        return get_pandas(self.format_name)

    def resolve_store_key(
        self,
        keys: list[str],
        *,
        dataset: str | None,
    ) -> str | None:
        """
        Resolve one selected HDF5 key from available keys.
        """
        if not keys:
            return None
        if dataset is not None:
            if dataset not in keys:
                raise ValueError(f'HDF5 dataset {dataset!r} not found')
            return dataset
        if DEFAULT_KEY in keys:
            return DEFAULT_KEY
        if len(keys) == 1:
            return keys[0]
        raise ValueError(
            'Multiple datasets found in HDF5 file; expected "data" or '
            'a single dataset',
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
        Reject writes for HDF5 while preserving scientific dataset contract.
        """
        _ = dataset
        return self.write(path, data, options=options)
