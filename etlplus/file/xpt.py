"""
:mod:`etlplus.file.xpt` module.

Helpers for reading/writing SAS Transport (XPT) files.

Notes
-----
- A SAS Transport (XPT) file is a standardized file format used to transfer
    SAS datasets between different systems.
- Common cases:
    - Sharing datasets between different SAS installations.
    - Archiving datasets in a platform-independent format.
    - Importing/exporting data to/from statistical software that supports XPT.
- Rule of thumb:
    - If you need to work with XPT files, use this module for reading
        and writing.
"""

from __future__ import annotations

from ._imports import get_dependency as _get_dependency
from ._imports import get_pandas as _get_pandas
from ._scientific_handlers import SingleDatasetTabularScientificReadWriteMixin
from ._statistical_handlers import PyreadstatReadSasFallbackFrameMixin
from ._statistical_handlers import PyreadstatRequiredWriteFrameMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'XptFile',
]


# SECTION: INTERNAL HELPERS ================================================= #


# Preserve module-level resolver hooks for contract tests.
get_dependency = _get_dependency
get_pandas = _get_pandas


# SECTION: CLASSES ========================================================== #


class XptFile(
    PyreadstatReadSasFallbackFrameMixin,
    PyreadstatRequiredWriteFrameMixin,
    SingleDatasetTabularScientificReadWriteMixin,
):
    """Handler implementation for XPT files."""

    # -- Class Attributes -- #

    format = FileFormat.XPT
    pyreadstat_mode = 'read_write'
    pyreadstat_read_method = 'read_xport'
    pyreadstat_write_method = 'write_xport'
    sas_format_hint = 'xport'
