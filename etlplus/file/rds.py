"""
:mod:`etlplus.file.rds` module.

Helpers for reading/writing R (RDS) data files.

Notes
-----
- An RDS file is a binary file format used by R to store a single R object,
    such as a data frame, list, or vector.
- Common cases:
    - Storing R objects for later use.
    - Sharing R data between users.
    - Loading R data into Python for analysis.
- Rule of thumb:
    - If the file follows the RDS specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from pathlib import Path

from ..utils.types import JSONData
from ._imports import get_dependency as _get_dependency
from ._imports import get_pandas as _get_pandas
from ._r_handlers import RDataHandlerMixin
from .base import ReadOptions
from .base import SingleDatasetScientificFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RdsFile',
]


# SECTION: INTERNAL HELPERS ================================================= #


# Preserve module-level resolver hooks for contract tests.
get_dependency = _get_dependency
get_pandas = _get_pandas


# SECTION: CLASSES ========================================================== #


class RdsFile(RDataHandlerMixin, SingleDatasetScientificFileHandlerABC):
    """
    Handler implementation for RDS files.
    """

    # -- Class Attributes -- #

    format = FileFormat.RDS

    # -- Instance Methods -- #

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return one dataset from RDS at *path*.

        Parameters
        ----------
        path : Path
            Path to the RDS file on disk.
        dataset : str | None, optional
            Dataset key to select. If omitted, default behavior is preserved.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed dataset payload.
        """
        dataset = self.resolve_dataset(dataset, options=options)
        return self.coerce_r_dataset(path, dataset=dataset)

    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write one dataset to RDS at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the RDS file on disk.
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
        frame = self.dataframe_from_records(records)
        count = len(records)

        writer = self.resolve_pyreadr_writer(
            'write_rds',
            error_message=(
                'RDS write support requires "pyreadr" with write_rds().'
            ),
        )
        self.call_pyreadr_writer(writer, path=path, frame=frame)
        return count
