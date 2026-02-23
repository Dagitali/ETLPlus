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
from ._imports import resolve_dependency
from ._imports import resolve_pandas as resolve_pandas_dependency
from ._io import ensure_parent_dir
from ._io import records_from_table
from .base import ReadOptions
from .base import SingleDatasetScientificFileHandlerABC
from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ScientificPandasResolverMixin',
    'ScientificXarrayResolverMixin',
    'SingleDatasetTabularScientificReadMixin',
    'SingleDatasetTabularScientificReadWriteMixin',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


# SECTION: CLASSES ========================================================== #


class ScientificPandasResolverMixin:
    """
    Shared pandas dependency resolver for scientific handlers.
    """

    format_name: ClassVar[str]

    def resolve_pandas(self) -> Any:
        """
        Return the pandas module for this handler.
        """
        return resolve_pandas_dependency(
            self,
            format_name=self.format_name,
        )


class ScientificXarrayResolverMixin:
    """
    Shared xarray dependency resolver for scientific handlers.
    """

    format_name: ClassVar[str]

    def resolve_xarray(self) -> Any:
        """
        Return the xarray module for this handler.
        """
        return resolve_dependency(
            self,
            'xarray',
            format_name=self.format_name,
        )


class SingleDatasetTabularScientificReadMixin(
    ScientificPandasResolverMixin,
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

        Parameters
        ----------
        path : Path
            The path to the file to read.
        pandas : Any
            The pandas module, passed via dependency injection.
        pyreadstat : Any | None
            The pyreadstat module, passed via dependency injection when
            required by the mixin. Will be None if not required.
        options : ReadOptions | None
            Optional read options.

        Returns
        -------
        Any
            The resulting dataframe-like object.

        Raises
        ------
        RuntimeError
            If the pyreadstat dependency is required but not provided.
        """

    # -- Internal Instance Methods -- #

    def _read_pyreadstat_dependency(self) -> Any | None:
        """
        Resolve the read-time pyreadstat dependency when required.

        Returns
        -------
        Any | None
            The pyreadstat module when required, else None.
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

    def resolve_pyreadstat(self) -> Any:
        """
        Return the pyreadstat module for this handler.
        """
        return resolve_dependency(
            self,
            'pyreadstat',
            format_name=self.format_name,
        )


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

        Returns
        -------
        Any | None
            The pyreadstat module when required, else None.
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

        Parameters
        ----------
        path : Path
            The path to the file to write.
        frame : Any
            The dataframe-like object to write.
        pandas : Any
            The pandas module, passed via dependency injection.
        pyreadstat : Any | None
            The pyreadstat module, passed via dependency injection when
            required by the mixin. Will be None if not required.
        options : WriteOptions | None
            Optional write options.
        """

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

        Parameters
        ----------
        path : Path
            The path to the file to write.
        data : JSONData
            The data to write.
        dataset : str | None, optional
            Dataset selector. Use the default dataset key or ``None``.
        options : WriteOptions | None, optional
            Optional write options.

        Returns
        -------
        int
            The number of records written.
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
