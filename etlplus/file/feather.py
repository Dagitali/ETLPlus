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

from typing import Any

from ._imports import get_dependency
from ._imports import get_pandas
from ._pandas_handlers import PandasColumnarHandlerMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'FeatherFile',
]

# SECTION: CLASSES ========================================================== #


class FeatherFile(PandasColumnarHandlerMixin):
    """
    Handler implementation for Feather files.
    """

    # -- Class Attributes -- #

    format = FileFormat.FEATHER
    engine_name = 'pandas'
    pandas_format_name = 'FEATHER'
    read_method = 'read_feather'
    write_method = 'to_feather'
    requires_pyarrow = True

    # -- Instance Methods -- #

    def resolve_pandas(self) -> Any:
        """
        Return pandas using the local dependency resolver hook.
        """
        return get_pandas(self.pandas_format_name)

    def resolve_pyarrow(self) -> Any:
        """
        Return pyarrow using the local dependency resolver hook.
        """
        return get_dependency('pyarrow', format_name=self.pandas_format_name)
