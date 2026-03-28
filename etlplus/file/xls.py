"""
:mod:`etlplus.file.xls` module.

Helpers for reading Excel XLS files (write is not supported).
"""

from __future__ import annotations

from ._enums import FileFormat
from ._imports import get_dependency  # noqa: F401
from ._imports import get_pandas  # noqa: F401
from ._pandas_handlers import PandasReadOnlySpreadsheetHandlerMixin

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'XlsFile',
]

# SECTION: CLASSES ========================================================== #


class XlsFile(PandasReadOnlySpreadsheetHandlerMixin):
    """Read-only handler implementation for XLS files."""

    # -- Class Attributes -- #

    format = FileFormat.XLS
    engine_name = 'xlrd'
    pandas_format_name = 'XLS'
    read_engine = 'xlrd'
