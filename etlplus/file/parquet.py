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

from ._enums import FileFormat
from ._imports import get_dependency  # noqa: F401
from ._imports import get_pandas  # noqa: F401
from ._pandas_handlers import PandasColumnarHandlerMixin

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ParquetFile',
]

# SECTION: CLASSES ========================================================== #


class ParquetFile(PandasColumnarHandlerMixin):
    """Handler implementation for Parquet files."""

    # -- Class Attributes -- #

    format = FileFormat.PARQUET
    engine_name = 'pandas'
    pandas_format_name = 'PARQUET'
    read_method = 'read_parquet'
    write_method = 'to_parquet'
    write_kwargs = (('index', False),)
    requires_pyarrow = True
