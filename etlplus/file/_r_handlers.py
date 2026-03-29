"""
:mod:`etlplus.file._r_handlers` module.

Shared abstractions for R-data handlers.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import ClassVar
from typing import Protocol

from ..utils._types import JSONData
from ..utils._types import JSONList
from ._dataframe import dataframe_and_count_from_data
from ._dataframe import dataframe_from_records
from ._imports import FormatPandasResolverMixin
from ._io import ensure_parent_dir
from ._r import coerce_r_result

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RDataHandlerMixin',
]


# SECTION: PROTOCOLS ======================================================== #


class _PyreadrModuleProtocol(Protocol):
    """Structural protocol for pyreadr modules used by R-data handlers."""

    def read_r(
        self,
        path: str,
    ) -> Mapping[str, object]:
        """Read one R-data payload mapping from a file path."""


# SECTION: CLASSES ========================================================== #


class RDataHandlerMixin(FormatPandasResolverMixin):
    """Shared dependency and payload helpers for R-data scientific handlers."""

    # -- Class Attributes -- #

    format_name: ClassVar[str]
    dataset_key: ClassVar[str]

    # -- Instance Methods -- #

    def call_pyreadr_writer(
        self,
        writer: Callable[..., Any],
        *,
        path: Path,
        frame: Any,
        kwargs: Mapping[str, object] | None = None,
    ) -> None:
        """
        Call one pyreadr writer with kwargs and compatibility fallback.

        Parameters
        ----------
        writer : Callable[..., Any]
            The pyreadr writer callable.
        path : Path
            Path to write.
        frame : Any
            Dataframe-like payload to write.
        kwargs : Mapping[str, object] | None, optional
            Optional keyword arguments to pass to the writer.
        """
        ensure_parent_dir(path)
        if kwargs is None:
            writer(str(path), frame)
            return
        try:
            writer(str(path), frame, **dict(kwargs))
        except TypeError:
            writer(str(path), frame)

    def coerce_r_dataset(
        self,
        path: Path,
        *,
        dataset: str | None,
    ) -> JSONData:
        """
        Read and normalize one selected R dataset payload from *path*.

        Parameters
        ----------
        path : Path
            The file path to read from.
        dataset : str | None
            The name of the dataset to select from the R data file. If None,
            the default dataset will be selected.

        Returns
        -------
        JSONData
            The normalized dataset payload.
        """
        return coerce_r_result(
            self.read_r_result(path),
            dataset=dataset,
            dataset_key=self.dataset_key,
            format_name=self.format_name,
            pandas=self.resolve_pandas(),
        )

    def dataframe_from_data(
        self,
        data: JSONData,
    ) -> tuple[Any, int]:
        """
        Normalize JSON-like payload and return dataframe plus record count.

        Parameters
        ----------
        data : JSONData
            The JSON-like payload to normalize.

        Returns
        -------
        tuple[Any, int]
            A tuple containing the dataframe and the record count.
        """
        return dataframe_and_count_from_data(
            self.resolve_pandas(),
            data,
            format_name=self.format_name,
        )

    def dataframe_from_records(
        self,
        records: JSONList,
    ) -> Any:
        """
        Build one pandas DataFrame from record payloads.

        Parameters
        ----------
        records : JSONList
            The list of record dictionaries to convert into a DataFrame.

        Returns
        -------
        Any
            The resulting pandas DataFrame.
        """
        return dataframe_from_records(
            self.resolve_pandas(),
            records,
        )

    def read_r_result(
        self,
        path: Path,
    ) -> Mapping[str, object]:
        """
        Read and return raw pyreadr result mapping from *path*.

        Parameters
        ----------
        path : Path
            The file path to read from.

        Returns
        -------
        Mapping[str, object]
            The raw pyreadr result mapping.
        """
        return self.resolve_pyreadr().read_r(str(path))

    def resolve_pyreadr(self) -> _PyreadrModuleProtocol:
        """
        Return pyreadr using shared dependency resolution.

        Returns
        -------
        _PyreadrModuleProtocol
            The pyreadr module.
        """
        return self.resolve_format_dependency('pyreadr')

    def resolve_pyreadr_writer(
        self,
        *method_names: str,
        error_message: str,
    ) -> Callable[..., Any]:
        """
        Resolve one callable pyreadr writer from candidate method names.

        Parameters
        ----------
        *method_names : str
            Candidate writer method names in priority order.
        error_message : str
            ImportError message to raise when no writer is available.

        Returns
        -------
        Callable[..., Any]
            The resolved pyreadr writer callable.

        Raises
        ------
        ImportError
            If none of the requested writer methods are available.
        """
        pyreadr = self.resolve_pyreadr()
        for method_name in method_names:
            writer = getattr(pyreadr, method_name, None)
            if callable(writer):
                return writer
        raise ImportError(error_message)
