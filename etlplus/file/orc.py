"""
:mod:`etlplus.file.orc` module.

Helpers for reading/writing Optimized Row Columnar (ORC) files.

Notes
-----
- An ORC file is a columnar storage file format optimized for Big Data
    processing.
- Common cases:
    - Efficient storage and retrieval of large datasets.
    - Integration with big data frameworks like Apache Hive and Apache Spark.
    - Compression and performance optimization for analytical queries.
- Rule of thumb:
    - If the file follows the ORC specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from ._imports import get_dependency  # noqa: F401
from ._imports import get_pandas  # noqa: F401
from ._pandas_handlers import PandasColumnarHandlerMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'OrcFile',
]

# SECTION: CLASSES ========================================================== #


class OrcFile(PandasColumnarHandlerMixin):
    """Handler implementation for ORC files."""

    # -- Class Attributes -- #

    format = FileFormat.ORC
    engine_name = 'pandas'
    pandas_format_name = 'ORC'
    read_method = 'read_orc'
    write_method = 'to_orc'
    write_kwargs = (('index', False),)
    requires_pyarrow = True
