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

from ..types import JSONData
from ..types import JSONList
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import read_sas_table
from ._io import records_from_table
from .base import ReadOnlyFileHandlerABC
from .base import ReadOptions
from .base import SingleDatasetScientificFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'Sas7bdatFile',
]


# SECTION: CLASSES ========================================================== #


class Sas7bdatFile(
    ReadOnlyFileHandlerABC,
    SingleDatasetScientificFileHandlerABC,
):
    """
    Read-only handler implementation for SAS7BDAT files.
    """

    # -- Class Attributes -- #

    format = FileFormat.SAS7BDAT

    # -- Instance Methods -- #

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return one dataset from SAS7BDAT at *path*.

        Parameters
        ----------
        path : Path
            Path to the SAS7BDAT file on disk.
        dataset : str | None, optional
            Dataset selector. Use the default dataset key or ``None``.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            Parsed records.
        """
        self.resolve_single_dataset(dataset, options=options)
        _ = get_dependency('pyreadstat', format_name=self.format_name)
        pandas = get_pandas(self.format_name)
        frame = read_sas_table(pandas, path, format_hint='sas7bdat')
        return records_from_table(frame)

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
