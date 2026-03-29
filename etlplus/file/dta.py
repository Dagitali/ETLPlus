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

from ._enums import FileFormat
from ._imports import get_dependency  # noqa: F401
from ._imports import get_pandas  # noqa: F401
from ._scientific_handlers import SingleDatasetTabularScientificReadWriteMixin
from ._statistical_handlers import PandasStataReadWriteFrameMixin

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'DtaFile',
]

# SECTION: CLASSES ========================================================== #


class DtaFile(
    PandasStataReadWriteFrameMixin,
    SingleDatasetTabularScientificReadWriteMixin,
):
    """Handler implementation for DTA files."""

    # -- Class Attributes -- #

    format = FileFormat.DTA
    pyreadstat_mode = 'read_write'
