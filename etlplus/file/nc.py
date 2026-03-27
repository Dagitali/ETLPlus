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
from typing import Any

from ..utils.types import JSONData
from ..utils.types import JSONList
from ._dataframe import dataframe_from_records
from ._imports import get_dependency  # noqa: F401
from ._imports import get_pandas  # noqa: F401
from ._imports import raise_engine_import_error
from ._io import ensure_parent_dir
from ._io import records_from_table
from ._scientific_handlers import ScientificPandasResolverMixin
from ._scientific_handlers import ScientificXarrayResolverMixin
from .base import ReadOptions
from .base import SingleDatasetScientificFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'NcFile',
]

# SECTION: CLASSES ========================================================== #


class NcFile(
    ScientificXarrayResolverMixin,
    ScientificPandasResolverMixin,
    SingleDatasetScientificFileHandlerABC,
):
    """Handler implementation for NC files."""

    # -- Class Attributes -- #

    format = FileFormat.NC

    # -- Instance Methods -- #

    def drop_sequential_index_column(
        self,
        frame: Any,
    ) -> Any:
        """Drop the index column when it is a simple 0..N-1 sequence."""
        if 'index' not in frame.columns:
            return frame
        values = list(frame['index'])
        if values != list(range(len(values))):
            return frame
        return frame.drop(columns=['index'])

    # -- Instance Methods -- #

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
        self.resolve_single_dataset(dataset, options=options)
        xarray = self.resolve_xarray()
        try:
            xarray_dataset = xarray.open_dataset(path)
        except ImportError as e:  # pragma: no cover
            raise_engine_import_error(
                e,
                format_name='NC',
                dependency_names=('netCDF4', 'h5netcdf'),
                pip_name='netCDF4',
            )
        with xarray_dataset as ds:
            frame = ds.to_dataframe().reset_index()
        frame = self.drop_sequential_index_column(frame)
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
        records = self.prepare_single_dataset_write_records(
            data,
            dataset=dataset,
            options=options,
        )
        if not records:
            return 0

        xarray = self.resolve_xarray()
        pandas = self.resolve_pandas()
        frame = dataframe_from_records(pandas, records)
        ds = xarray.Dataset.from_dataframe(frame)
        ensure_parent_dir(path)
        try:
            ds.to_netcdf(path)
        except ImportError as e:  # pragma: no cover
            raise_engine_import_error(
                e,
                format_name='NC',
                dependency_names=('netCDF4', 'h5netcdf'),
                pip_name='netCDF4',
            )
        return len(records)
