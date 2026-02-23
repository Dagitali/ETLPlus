"""
:mod:`etlplus.file._scientific_handlers` module.

Shared abstractions for scientific dataset handlers.
"""

from __future__ import annotations

from abc import abstractmethod
from pathlib import Path
from typing import Any
from typing import ClassVar

from ..types import JSONData
from ..types import JSONList
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import ensure_parent_dir
from ._io import records_from_table
from .base import ReadOptions
from .base import SingleDatasetScientificFileHandlerABC
from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SingleDatasetTabularScientificReadMixin',
    'SingleDatasetTabularScientificReadWriteMixin',
]


# SECTION: CLASSES ========================================================== #


class SingleDatasetTabularScientificReadMixin(
    SingleDatasetScientificFileHandlerABC,
):
    """
    Shared read implementation for single-dataset tabular scientific formats.
    """

    requires_pyreadstat_for_read: ClassVar[bool] = False

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def read_frame(
        self,
        path: Path,
        *,
        pandas: Any,
        pyreadstat: Any | None,
        options: ReadOptions | None = None,
    ) -> Any:
        """
        Read and return one dataframe-like object from *path*.
        """

    # -- Internal Instance Methods -- #

    def _read_pyreadstat_dependency(self) -> Any | None:
        """
        Resolve the read-time pyreadstat dependency when required.
        """
        if not self.requires_pyreadstat_for_read:
            return None
        return self.resolve_pyreadstat()

    # -- Instance Methods -- #

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return one selected dataset as records.
        """
        self.resolve_single_dataset(dataset, options=options)
        frame = self.read_frame(
            path,
            pandas=self.resolve_pandas(),
            pyreadstat=self._read_pyreadstat_dependency(),
            options=options,
        )
        return records_from_table(frame)

    def resolve_pandas(self) -> Any:
        """
        Return the pandas module for this handler.
        """
        return get_pandas(self.format_name)

    def resolve_pyreadstat(self) -> Any:
        """
        Return the pyreadstat module for this handler.
        """
        return get_dependency('pyreadstat', format_name=self.format_name)


class SingleDatasetTabularScientificReadWriteMixin(
    SingleDatasetTabularScientificReadMixin,
):
    """
    Shared read/write implementation for single-dataset tabular scientific
    formats.
    """

    requires_pyreadstat_for_write: ClassVar[bool] = False

    # -- Internal Instance Methods -- #

    def _write_pyreadstat_dependency(self) -> Any | None:
        """
        Resolve the write-time pyreadstat dependency when required.
        """
        if not self.requires_pyreadstat_for_write:
            return None
        return self.resolve_pyreadstat()

    # -- Abstract Instance Methods -- #

    @abstractmethod
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
        Write one dataframe-like object to *path*.
        """

    # -- Instance Methods -- #

    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write one selected dataset and return record count.
        """
        records = self.prepare_single_dataset_write_records(
            data,
            dataset=dataset,
            options=options,
        )
        if not records:
            return 0

        pandas = self.resolve_pandas()
        ensure_parent_dir(path)
        frame = pandas.DataFrame.from_records(records)
        self.write_frame(
            path,
            frame,
            pandas=pandas,
            pyreadstat=self._write_pyreadstat_dependency(),
            options=options,
        )
        return len(records)
