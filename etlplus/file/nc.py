"""
:mod:`etlplus.file.nc` module.

Helpers for reading/writing NetCDF (NC) data files.

Notes
-----
- A NC file is a binary file format used for array-oriented scientific data,
    particularly in meteorology, oceanography, and climate science.
- Common cases:
    - Storing multi-dimensional scientific data.
    - Sharing large datasets in research communities.
    - Efficient data access and manipulation.
- Rule of thumb:
    - If the file follows the NetCDF standard, use this module for reading and
        writing.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._io import warn_deprecated_module_io
from .base import ReadOptions
from .base import SingleDatasetScientificFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'NcFile',
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _raise_engine_error(
    err: ImportError,
) -> None:
    """
    Raise a consistent ImportError for missing NetCDF engine support.

    Parameters
    ----------
    err : ImportError
        The original ImportError raised when trying to use NetCDF support
        without the required dependency.

    Raises
    ------
    ImportError
        Consistent ImportError indicating that NetCDF support requires
        optional dependencies.
    """
    raise ImportError(
        'NC support requires optional dependency "netCDF4" or "h5netcdf".\n'
        'Install with: pip install netCDF4',
    ) from err


# SECTION: CLASSES ========================================================== #


class NcFile(SingleDatasetScientificFileHandlerABC):
    """
    Handler implementation for NC files.
    """

    # -- Class Attributes -- #

    format = FileFormat.NC
    dataset_key = 'data'

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read NC content from *path*.

        Parameters
        ----------
        path : Path
            Path to the NC file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the NC file.
        """
        return cast(
            JSONList,
            self.read_dataset(path, options=options),
        )

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return one dataset from NC at *path*.

        Parameters
        ----------
        path : Path
            Path to the NC file on disk.
        dataset : str | None, optional
            Dataset selector. Use the default dataset key or ``None``.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            Parsed records.
        """
        dataset = self.resolve_read_dataset(dataset, options=options)
        self.validate_single_dataset_key(dataset)
        xarray = get_dependency('xarray', format_name='NC')
        try:
            xarray_dataset = xarray.open_dataset(path)
        except ImportError as err:  # pragma: no cover
            _raise_engine_error(err)
        with xarray_dataset as ds:
            frame = ds.to_dataframe().reset_index()
        if 'index' in frame.columns:
            values = list(frame['index'])
            if values == list(range(len(values))):
                frame = frame.drop(columns=['index'])
        return cast(JSONList, frame.to_dict(orient='records'))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to NC file at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the NC file on disk.
        data : JSONData
            Data to write as NC file. Should be a list of dictionaries or a
            single dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the NC file.
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
        Write one dataset to NC at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the NC file on disk.
        data : JSONData
            Dataset payload to write.
        dataset : str | None, optional
            Dataset selector. Use the default dataset key or ``None``.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of records written.
        """
        dataset = self.resolve_write_dataset(dataset, options=options)
        self.validate_single_dataset_key(dataset)

        records = normalize_records(data, 'NC')
        if not records:
            return 0

        xarray = get_dependency('xarray', format_name='NC')
        pandas = get_pandas('NC')
        frame = pandas.DataFrame.from_records(records)
        ds = xarray.Dataset.from_dataframe(frame)
        ensure_parent_dir(path)
        try:
            ds.to_netcdf(path)
        except ImportError as err:  # pragma: no cover
            _raise_engine_error(err)
        return len(records)


# SECTION: INTERNAL CONSTANTS =============================================== #

_NC_HANDLER = NcFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``NcFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the NC file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the NC file.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _NC_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``NcFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the NC file on disk.
    data : JSONData
        Data to write as NC file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the NC file.
    """
    warn_deprecated_module_io(__name__, 'write')
    return _NC_HANDLER.write(coerce_path(path), data)
