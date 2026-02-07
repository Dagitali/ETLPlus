"""
:mod:`etlplus.file.parquet` module.

Helpers for reading/writing Apache Parquet (PARQUET) files.

Notes
-----
- An Apache Parquet file is a columnar storage file format optimized for Big
    Data processing.
- Common cases:
    - Efficient storage and retrieval of large datasets.
    - Integration with big data frameworks like Apache Hive and Apache Spark.
    - Compression and performance optimization for analytical queries.
- Rule of thumb:
    - If the file follows the Apache Parquet specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_pandas
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import normalize_records
from .base import ColumnarFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

if TYPE_CHECKING:
    import pyarrow

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ParquetFile',
    # Functions
    'read',
    'write',
]

# SECTION: CLASSES ========================================================== #


class ParquetFile(ColumnarFileHandlerABC):
    """
    Handler implementation for Parquet files.
    """

    # -- Class Attributes -- #

    format = FileFormat.PARQUET
    engine_name = 'pandas'

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return Parquet content from *path*.

        Parameters
        ----------
        path : Path
            Path to the Parquet file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the Parquet file.
        """
        table = self.read_table(path, options=options)
        return self.table_to_records(table)

    def read_table(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> pyarrow.Table:
        """
        Read a Parquet table object from *path*.

        Parameters
        ----------
        path : Path
            Path to the Parquet file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        pyarrow.Table
            PyArrow table object.

        Raises
        ------
        ImportError
            If the required optional dependency for Parquet support is not
            installed.
        """
        _ = options
        pandas = get_pandas('Parquet')
        try:
            return pandas.read_parquet(path)
        except ImportError as err:  # pragma: no cover
            raise ImportError(
                'Parquet support requires optional dependency '
                '"pyarrow" or "fastparquet".\n'
                'Install with: pip install pyarrow',
            ) from err

    def records_to_table(
        self,
        data: JSONData,
    ) -> pyarrow.Table:
        """
        Convert row-oriented records into a Parquet table object.

        Parameters
        ----------
        data : JSONData
            Records to convert.

        Returns
        -------
        pyarrow.Table
            PyArrow table object.
        """
        records = normalize_records(data, 'Parquet')
        pandas = get_pandas('Parquet')
        return pandas.DataFrame.from_records(records)

    def table_to_records(
        self,
        table: pyarrow.Table,
    ) -> JSONList:
        """
        Convert a Parquet table object into row-oriented records.

        Parameters
        ----------
        table : pyarrow.Table
            PyArrow table object.

        Returns
        -------
        JSONList
            Parsed records.
        """
        return cast(JSONList, table.to_dict(orient='records'))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to Parquet at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the Parquet file on disk.
        data : JSONData
            Data to write.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of records written.
        """
        records = normalize_records(data, 'Parquet')
        if not records:
            return 0

        ensure_parent_dir(path)
        table = self.records_to_table(records)
        self.write_table(path, table, options=options)
        return len(records)

    def write_table(
        self,
        path: Path,
        table: pyarrow.Table,
        *,
        options: WriteOptions | None = None,
    ) -> None:
        """
        Write a Parquet table object to *path*.

        Parameters
        ----------
        path : Path
            Path to the Parquet file on disk.
        table : pyarrow.Table
            PyArrow table object.
        options : WriteOptions | None, optional
            Optional write parameters.

        Raises
        ------
        ImportError
            If the required optional dependency for Parquet support is not
            installed.
        """
        _ = options
        try:
            table.to_parquet(path, index=False)
        except ImportError as err:  # pragma: no cover
            raise ImportError(
                'Parquet support requires optional dependency '
                '"pyarrow" or "fastparquet".\n'
                'Install with: pip install pyarrow',
            ) from err


# SECTION: INTERNAL CONSTANTS =============================================== #

_PARQUET_HANDLER = ParquetFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read and return Parquet content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the PARQUET file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the Parquet file.
    """
    return _PARQUET_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to Parquet at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the PARQUET file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.
    """
    return _PARQUET_HANDLER.write(coerce_path(path), data)
