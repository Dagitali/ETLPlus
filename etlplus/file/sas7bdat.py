"""
:mod:`etlplus.file.sas7bdat` module.

Helpers for reading SAS (SAS7BDAT) data files.

Notes
-----
- A SAS7BDAT file is a proprietary binary file format created by SAS to store
    datasets, including variables, labels, and data types.
- Common cases:
    - Statistical analysis pipelines.
    - Data exchange with SAS tooling.
- Rule of thumb:
    - If the file follows the SAS7BDAT specification, use this module for
        reading.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..types import JSONData
from ._imports import get_dependency as _get_dependency
from ._imports import get_pandas as _get_pandas
from ._io import read_sas_table
from ._scientific_handlers import SingleDatasetTabularScientificReadMixin
from .base import ReadOnlyFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'Sas7bdatFile',
]


# SECTION: INTERNAL HELPERS ================================================= #


# Preserve module-level resolver hooks for contract tests.
get_dependency = _get_dependency
get_pandas = _get_pandas


# SECTION: CLASSES ========================================================== #


class Sas7bdatFile(
    ReadOnlyFileHandlerABC,
    SingleDatasetTabularScientificReadMixin,
):
    """
    Read-only handler implementation for SAS7BDAT files.
    """

    # -- Class Attributes -- #

    format = FileFormat.SAS7BDAT
    requires_pyreadstat_for_read = True

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
        Read and return one dataframe-like dataset from SAS7BDAT.
        """
        _ = pyreadstat
        _ = options
        return read_sas_table(pandas, path, format_hint='sas7bdat')

    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Reject writes for SAS7BDAT while preserving scientific dataset
        contract.
        """
        self.resolve_single_dataset(dataset, options=options)
        return self.write(path, data, options=options)
