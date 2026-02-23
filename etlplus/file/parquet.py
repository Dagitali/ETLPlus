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
from typing import Any

from ..types import JSONData
from ..types import JSONList
from ._imports import get_pandas
from ._io import make_deprecated_module_io
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


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _pandas() -> Any:
    """Return the optional pandas module for Parquet operations."""
    return get_pandas('Parquet')


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
    ) -> Any:
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
        Any
            Columnar table object.

        Raises
        ------
        ImportError
            If the required optional dependency for Parquet support is not
            installed.
        """
        _ = options
        pandas = _pandas()
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
    ) -> Any:
        """
        Convert row-oriented records into a Parquet table object.

        Parameters
        ----------
        data : JSONData
            Records to convert.

        Returns
        -------
        Any
            Columnar table object.
        """
        records = normalize_records(data, 'Parquet')
        pandas = _pandas()
        return pandas.DataFrame.from_records(records)

    def table_to_records(
        self,
        table: Any,
    ) -> JSONList:
        """
        Convert a Parquet table object into row-oriented records.

        Parameters
        ----------
        table : Any
            Columnar table object.

        Returns
        -------
        JSONList
            Parsed records.
        """
        return records_from_table(table)

    def write_table(
        self,
        path: Path,
        table: Any,
        *,
        options: WriteOptions | None = None,
    ) -> None:
        """
        Write a Parquet table object to *path*.

        Parameters
        ----------
        path : Path
            Path to the Parquet file on disk.
        table : Any
            Columnar table object.
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


read, write = make_deprecated_module_io(__name__, _PARQUET_HANDLER)
