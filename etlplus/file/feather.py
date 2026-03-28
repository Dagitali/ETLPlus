"""
:mod:`etlplus.file.feather` module.

Helpers for reading/writing Apache Arrow Feather (FEATHER) files.

Notes
-----
- A FEATHER file is a binary file format designed for efficient
    on-disk storage of data frames, built on top of Apache Arrow.
- Common cases:
    - Fast read/write operations for data frames.
    - Interoperability between different data analysis tools.
    - Storage of large datasets with efficient compression.
- Rule of thumb:
    - If the file follows the Apache Arrow Feather specification, use this
        module for reading and writing.
"""

from __future__ import annotations

from ._enums import FileFormat
from ._imports import get_dependency  # noqa: F401
from ._imports import get_pandas  # noqa: F401
from ._pandas_handlers import PandasColumnarHandlerMixin

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'FeatherFile',
]

# SECTION: CLASSES ========================================================== #


class FeatherFile(PandasColumnarHandlerMixin):
    """Handler implementation for Feather files."""

    # -- Class Attributes -- #

    format = FileFormat.FEATHER
    engine_name = 'pandas'
    pandas_format_name = 'FEATHER'
    read_method = 'read_feather'
    write_method = 'to_feather'
    requires_pyarrow = True
