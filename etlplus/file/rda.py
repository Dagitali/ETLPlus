"""
:mod:`etlplus.file.rda` module.

Helpers for reading/writing RData workspace/object bundle (RDA) files.

Notes
-----
- A RDA file is a binary file format used by R to store workspace objects,
    including data frames, lists, and other R objects.
- Common cases:
    - Storing R data objects for later use.
    - Sharing R datasets between users.
    - Loading R data into Python for analysis.
- Rule of thumb:
    - If the file follows the RDA specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from pathlib import Path

from ..utils.types import JSONData
from ._imports import get_dependency as _get_dependency
from ._imports import get_pandas as _get_pandas
from ._r import list_r_dataset_keys
from ._r_handlers import RDataHandlerMixin
from .base import ReadOptions
from .base import ScientificDatasetFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RdaFile',
]


# SECTION: INTERNAL HELPERS ================================================= #


# Preserve module-level resolver hooks for contract tests.
get_dependency = _get_dependency
get_pandas = _get_pandas


# SECTION: CLASSES ========================================================== #


class RdaFile(RDataHandlerMixin, ScientificDatasetFileHandlerABC):
    """
    Handler implementation for RDA files.
    """

    # -- Class Attributes -- #

    format = FileFormat.RDA

    # -- Instance Methods -- #

    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:
        """
        Return available dataset keys in an RDA container.

        Parameters
        ----------
        path : Path
            Path to the RDA file on disk.

        Returns
        -------
        list[str]
            Available dataset keys.
        """
        result = self.read_r_result(path)
        return list_r_dataset_keys(
            result,
            default_key=self.dataset_key,
        )

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read one dataset (or all datasets) from RDA at *path*.

        Parameters
        ----------
        path : Path
            Path to the RDA file on disk.
        dataset : str | None, optional
            Dataset key to select. If omitted, all objects are returned.
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
        Write one dataset to RDA at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the RDA file on disk.
        data : JSONData
            Dataset payload to write.
        dataset : str | None, optional
            Target dataset key. Defaults to :attr:`dataset_key`.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of records written.

        Raises
        ------
        ImportError
            If "pyreadr" is not installed with write support.
        """
        dataset = self.resolve_dataset(dataset, options=options)
        frame, count = self.dataframe_from_data(data)
        target_dataset = dataset if dataset is not None else self.dataset_key

        writer = self.resolve_pyreadr_writer(
            'write_rdata',
            'write_rda',
            error_message=(
                'RDA write support requires "pyreadr" with write_rdata().'
            ),
        )
        self.call_pyreadr_writer(
            writer,
            path=path,
            frame=frame,
            kwargs={'df_name': target_dataset},
        )
        return count
