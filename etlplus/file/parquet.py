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

from typing import Any

from ._imports import get_pandas
from ._pandas_handlers import PARQUET_DEPENDENCY_ERROR
from ._pandas_handlers import PandasColumnarHandlerMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ParquetFile',
]

# SECTION: CLASSES ========================================================== #


class ParquetFile(PandasColumnarHandlerMixin):
    """
    Handler implementation for Parquet files.
    """

    # -- Class Attributes -- #

    format = FileFormat.PARQUET
    engine_name = 'pandas'
    pandas_format_name = 'PARQUET'
    read_method = 'read_parquet'
    write_method = 'to_parquet'
    write_kwargs = (('index', False),)
    import_error_message = PARQUET_DEPENDENCY_ERROR

    # -- Internal Instance Methods -- #

    def resolve_pandas(self) -> Any:
        """
        Return pandas using the local dependency resolver hook.
        """
        return get_pandas(self.pandas_format_name)
