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
from ._io import make_deprecated_module_io
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
    # Functions
    'read',
    'write',
]


# SECTION: CONSTANTS ======================================================== #


DEFAULT_KEY = 'data'


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _pandas() -> Any:
    """Return the optional pandas module for HDF5 operations."""
    return get_pandas('HDF5')


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
        pandas = _pandas()
        try:
            store = pandas.HDFStore(path)
        except ImportError as err:  # pragma: no cover
            _raise_tables_error(err)
        with store:
            return [key.lstrip('/') for key in store.keys()]

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
        pandas = _pandas()
        try:
            store = pandas.HDFStore(path)
        except ImportError as err:  # pragma: no cover
            _raise_tables_error(err)

        with store:
            keys = [key.lstrip('/') for key in store.keys()]
            if not keys:
                return []
            if dataset is not None:
                if dataset not in keys:
                    raise ValueError(f'HDF5 dataset {dataset!r} not found')
                key = dataset
            elif DEFAULT_KEY in keys:
                key = DEFAULT_KEY
            elif len(keys) == 1:
                key = keys[0]
            else:
                raise ValueError(
                    'Multiple datasets found in HDF5 file; expected "data" or '
                    'a single dataset',
                )
            frame = store.get(key)
        return records_from_table(frame)

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


# SECTION: INTERNAL CONSTANTS =============================================== #


_HDF5_HANDLER = Hdf5File()


# SECTION: FUNCTIONS ======================================================== #


read, write = make_deprecated_module_io(__name__, _HDF5_HANDLER)
