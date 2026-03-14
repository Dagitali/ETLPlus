"""
:mod:`etlplus.file.ods` module.

Helpers for reading/writing OpenDocument (ODS) spreadsheet files.

Notes
-----
- An ODS file is a spreadsheet file created using the OpenDocument format.
- Common cases:
    - Spreadsheet files created by LibreOffice Calc, Apache OpenOffice Calc, or
        other applications that support the OpenDocument format.
    - Spreadsheet files exchanged in open standards environments.
    - Spreadsheet files used in government or educational institutions
        promoting open formats.
- Rule of thumb:
    - If the file follows the OpenDocument specification, use this module for
        reading and writing.
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
    'OdsFile',
]

# SECTION: CLASSES ========================================================== #


class OdsFile(PandasSpreadsheetHandlerMixin):
    """Handler implementation for ODS files."""

    # -- Class Attributes -- #

    format = FileFormat.ODS
    engine_name = 'odf'
    pandas_format_name = 'ODS'
    read_engine = 'odf'
    write_engine = 'odf'
