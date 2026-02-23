"""
:mod:`etlplus.file.xls` module.

Helpers for reading Excel XLS files (write is not supported).
"""

from __future__ import annotations

from ._imports import get_pandas as _get_pandas
from ._pandas_handlers import PandasReadOnlySpreadsheetHandlerMixin
from .enums import FileFormat

# Keep module-level resolver hook for monkeypatch-driven contract tests.
get_pandas = _get_pandas

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
