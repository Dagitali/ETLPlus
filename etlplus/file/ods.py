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

from typing import Any

from ._imports import get_pandas
from ._pandas_handlers import PandasSpreadsheetHandlerMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'OdsFile',
]

# SECTION: CLASSES ========================================================== #


class OdsFile(PandasSpreadsheetHandlerMixin):
    """
    Handler implementation for ODS files.
    """

    # -- Class Attributes -- #

    format = FileFormat.ODS
    engine_name = 'odf'
    pandas_format_name = 'ODS'
    read_engine = 'odf'
    write_engine = 'odf'
    import_error_message = (
        'ODS support requires optional dependency "odfpy".\n'
        'Install with: pip install odfpy'
    )

    # -- Internal Instance Methods -- #

    def resolve_pandas(self) -> Any:
        """
        Return pandas using the local dependency resolver hook.
        """
        return get_pandas(self.pandas_format_name)
