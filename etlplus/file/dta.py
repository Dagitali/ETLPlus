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

from pathlib import Path
from typing import Any

from ._imports import get_dependency
from ._imports import get_pandas
from ._scientific_handlers import SingleDatasetTabularScientificReadWriteMixin
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'DtaFile',
]


# SECTION: CLASSES ========================================================== #


class DtaFile(SingleDatasetTabularScientificReadWriteMixin):
    """
    Handler implementation for DTA files.
    """

    # -- Class Attributes -- #

    format = FileFormat.DTA
    requires_pyreadstat_for_read = True
    requires_pyreadstat_for_write = True

    # -- Instance Methods -- #

    def read_frame(
        self,
        path: Path,
        *,
        pandas: Any,
        pyreadstat: Any | None,
        options: ReadOptions | None = None,
    ) -> Any:
        """
        Read and return one dataframe-like dataset from DTA.
        """
        _ = pyreadstat
        _ = options
        return pandas.read_stata(path)

    def resolve_pandas(self) -> Any:
        """
        Return pandas using module-level dependency resolution.
        """
        return get_pandas(self.format_name)

    def resolve_pyreadstat(self) -> Any:
        """
        Return pyreadstat using module-level dependency resolution.
        """
        return get_dependency('pyreadstat', format_name=self.format_name)

    def write_frame(
        self,
        path: Path,
        frame: Any,
        *,
        pandas: Any,
        pyreadstat: Any | None,
        options: WriteOptions | None = None,
    ) -> None:
        """
        Write one dataframe-like dataset to DTA.
        """
        _ = pandas
        _ = pyreadstat
        _ = options
        frame.to_stata(path, write_index=False)
