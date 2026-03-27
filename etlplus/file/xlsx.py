"""
:mod:`etlplus.file.xlsx` module.

Helpers for reading/writing Excel XLSX files.
"""

from __future__ import annotations

from ._imports import get_dependency  # noqa: F401
from ._imports import get_pandas  # noqa: F401
from ._pandas_handlers import PandasSpreadsheetHandlerMixin
from .enums import FileFormat

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
