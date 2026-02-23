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

from ..types import JSONData
from ..types import JSONList
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import ensure_parent_dir
from ._io import records_from_table
from .base import ReadOptions
from .base import SingleDatasetScientificFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'DtaFile',
]


# SECTION: CLASSES ========================================================== #


class DtaFile(SingleDatasetScientificFileHandlerABC):
    """
    Handler implementation for DTA files.
    """

    # -- Class Attributes -- #

    format = FileFormat.DTA

    # -- Instance Methods -- #

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return one dataset from DTA at *path*.

        Parameters
        ----------
        path : Path
            Path to the DTA file on disk.
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
        frame = pandas.read_stata(path)
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
        Write one dataset to DTA at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the DTA file on disk.
        data : JSONData
            Dataset payload to write.
        dataset : str | None, optional
            Dataset selector. Use the default dataset key or ``None``.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of records written.
        """
        records = self.prepare_single_dataset_write_records(
            data,
            dataset=dataset,
            options=options,
        )
        if not records:
            return 0

        _ = get_dependency('pyreadstat', format_name=self.format_name)
        pandas = get_pandas(self.format_name)
        ensure_parent_dir(path)
        frame = pandas.DataFrame.from_records(records)
        frame.to_stata(path, write_index=False)
        return len(records)
