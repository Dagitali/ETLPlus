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

import pyarrow  # type: ignore[import]

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_pandas
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import normalize_records
from ._io import records_from_table
from .base import ColumnarFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

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
        return records_from_table(table)

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
    Deprecated wrapper. Use ``ParquetFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the PARQUET file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the Parquet file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _PARQUET_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``ParquetFile().write(...)`` instead.

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
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _PARQUET_HANDLER.write,
    )
