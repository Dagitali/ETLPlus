"""
:mod:`etlplus.file.xls` module.

Helpers for reading Excel XLS files (write is not supported).
"""

from __future__ import annotations

from typing import Any

from ._imports import get_pandas
from ._pandas_handlers import PandasReadOnlySpreadsheetHandlerMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'XlsFile',
]

# SECTION: CLASSES ========================================================== #


class XlsFile(PandasReadOnlySpreadsheetHandlerMixin):
    """
    Read-only handler implementation for XLS files.
    """

    # -- Class Attributes -- #

    format = FileFormat.XLS
    engine_name = 'xlrd'
    pandas_format_name = 'XLS'
    read_engine = 'xlrd'
    import_error_message = (
        'XLS support requires optional dependency "xlrd".\n'
        'Install with: pip install xlrd'
    )

    # -- Internal Instance Methods -- #

    def resolve_pandas(self) -> Any:
        """
        Return pandas using the local dependency resolver hook.
        """
        return get_pandas(self.pandas_format_name)
