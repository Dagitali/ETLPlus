"""
:mod:`etlplus.file.sav` module.

Helpers for reading/writing SPSS (SAV) files.

Notes
-----
- A SAV file is a dataset created by SPSS.
- Common cases:
    - Survey and market research datasets.
    - Statistical analysis workflows.
    - Exchange with SPSS and compatible tools.
- Rule of thumb:
    - If the file follows the SAV specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from ._imports import get_dependency as _get_dependency
from ._imports import get_pandas as _get_pandas
from ._scientific_handlers import SingleDatasetTabularScientificReadWriteMixin
from ._statistical_handlers import PyreadstatReadWriteFrameMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SavFile',
]


# SECTION: INTERNAL HELPERS ================================================= #


# Preserve module-level resolver hooks for contract tests.
get_dependency = _get_dependency
get_pandas = _get_pandas


# SECTION: CLASSES ========================================================== #


class SavFile(
    PyreadstatReadWriteFrameMixin,
    SingleDatasetTabularScientificReadWriteMixin,
):
    """
    Handler implementation for SAV files.
    """

    # -- Class Attributes -- #

    format = FileFormat.SAV
    pyreadstat_mode = 'read_write'
    pyreadstat_read_method = 'read_sav'
    pyreadstat_write_method = 'write_sav'
