"""
:mod:`etlplus.file.xlsx` module.

Helpers for reading/writing Excel XLSX files.
"""

from __future__ import annotations

from ._imports import get_dependency as _get_dependency
from ._imports import get_pandas as _get_pandas
from ._pandas_handlers import PandasSpreadsheetHandlerMixin
from .enums import FileFormat

# Keep module-level resolver hooks for monkeypatch-driven contract tests.
get_dependency = _get_dependency
get_pandas = _get_pandas

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'XlsxFile',
]

# SECTION: CLASSES ========================================================== #


class XlsxFile(PandasSpreadsheetHandlerMixin):
    """Handler implementation for XLSX files."""

    # -- Class Attributes -- #

    format = FileFormat.XLSX
    engine_name = 'openpyxl'
    pandas_format_name = 'XLSX'
    read_engine = 'openpyxl'
    write_engine = 'openpyxl'
