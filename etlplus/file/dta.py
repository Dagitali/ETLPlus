"""
:mod:`etlplus.file.dta` module.

Helpers for reading/writing Stata (DTA) files.

Notes
-----
- A DTA file is a proprietary binary format created by Stata to store datasets
    with variables, labels, and data types.
- Common cases:
    - Statistical analysis workflows.
    - Data sharing in research environments.
    - Interchange between Stata and other analytics tools.
- Rule of thumb:
    - If the file follows the DTA specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from ._imports import get_dependency as _get_dependency
from ._imports import get_pandas as _get_pandas
from ._scientific_handlers import SingleDatasetTabularScientificReadWriteMixin
from ._statistical_handlers import PandasStataReadWriteFrameMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'DtaFile',
]


# SECTION: INTERNAL HELPERS ================================================= #


# Preserve module-level resolver hooks for contract tests.
get_dependency = _get_dependency
get_pandas = _get_pandas


# SECTION: CLASSES ========================================================== #


class DtaFile(
    PandasStataReadWriteFrameMixin,
    SingleDatasetTabularScientificReadWriteMixin,
):
    """
    Handler implementation for DTA files.
    """

    # -- Class Attributes -- #

    format = FileFormat.DTA
    pyreadstat_mode = 'read_write'
