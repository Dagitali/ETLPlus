"""
:mod:`etlplus.file.xlsx` module.

Helpers for reading/writing Excel XLSX files.
"""

from __future__ import annotations

from typing import Any

from ._imports import get_pandas
from ._pandas_handlers import PandasSpreadsheetHandlerMixin
from .enums import FileFormat

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
        'XLSX support requires optional dependency "openpyxl".\n'
        'Install with: pip install openpyxl'
    )

    # -- Internal Instance Methods -- #

    def resolve_pandas(self) -> Any:
        """
        Return pandas using the local dependency resolver hook.
        """
        return get_pandas(self.pandas_format_name)
