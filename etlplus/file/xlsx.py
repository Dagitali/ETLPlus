"""
:mod:`etlplus.file.xlsx` module.

Helpers for reading/writing Excel XLSX files.
"""

from __future__ import annotations

from ._imports import get_pandas as _get_pandas
from ._pandas_handlers import PandasSpreadsheetHandlerMixin
from .enums import FileFormat

# Keep module-level resolver hook for monkeypatch-driven contract tests.
get_pandas = _get_pandas

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'XlsxFile',
]

# SECTION: CLASSES ========================================================== #


class XlsxFile(PandasSpreadsheetHandlerMixin):
    """
    Handler implementation for XLSX files.
    """

    # -- Class Attributes -- #

    format = FileFormat.XLSX
    engine_name = 'openpyxl'
    pandas_format_name = 'XLSX'
    import_error_message = (
        'XLSX support requires dependency "openpyxl".\n'
        'Install with: pip install openpyxl'
    )
