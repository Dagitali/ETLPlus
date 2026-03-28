"""
:mod:`etlplus.file.xlsm` module.

Helpers for reading/writing Microsoft Excel Macro-Enabled (XLSM)
spreadsheet files.

Notes
-----
- An XLSM file is a spreadsheet file created using the Microsoft Excel Macro-
    Enabled (Open XML) format.
- Common cases:
    - Reading data from Excel Macro-Enabled spreadsheets.
    - Writing data to Excel Macro-Enabled format for compatibility.
    - Converting XLSM files to more modern formats.
- Rule of thumb:
    - If you need to work with Excel Macro-Enabled spreadsheet files, use this
        module for reading and writing.
"""

from __future__ import annotations

from ._enums import FileFormat
from ._imports import get_dependency  # noqa: F401
from ._imports import get_pandas  # noqa: F401
from ._pandas_handlers import PandasSpreadsheetHandlerMixin

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'XlsmFile',
]

# SECTION: CLASSES ========================================================== #


class XlsmFile(PandasSpreadsheetHandlerMixin):
    """Handler implementation for XLSM files."""

    # -- Class Attributes -- #

    format = FileFormat.XLSM
    engine_name = 'openpyxl'
    pandas_format_name = 'XLSM'
    read_engine = 'openpyxl'
    write_engine = 'openpyxl'
