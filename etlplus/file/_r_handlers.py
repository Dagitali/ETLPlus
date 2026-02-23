"""
:mod:`etlplus.file._r_handlers` module.

Shared abstractions for R-data handlers.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import ClassVar
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import normalize_records
from ._r import coerce_r_result

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RDataHandlerMixin',
]


# SECTION: CLASSES ========================================================== #


def _resolve_module_callable(
    handler: object,
    name: str,
) -> Callable[..., Any] | None:
    """
    Resolve one callable from the concrete handler module when present.

    handler : object
        The handler instance to resolve the callable for.
    name : str
        The name of the callable to resolve.

    Returns
    -------
    Callable[..., Any] | None
        The resolved callable if found and callable, else ``None``.
    """
    module = sys.modules.get(type(handler).__module__)
    if module is None:
        return None
    value = getattr(module, name, None)
    return value if callable(value) else None


def _resolve_pandas_dependency(
    handler: object,
    *,
    format_name: str,
) -> Any:
    """
    Resolve pandas, preferring the concrete module resolver when present.

    Parameters
    ----------
    handler : object
        The handler instance to resolve the pandas dependency for.
    format_name : str
        The name of the format to resolve pandas for.

    Returns
    -------
    Any
        The resolved pandas module.
    """
    if resolver := _resolve_module_callable(handler, 'get_pandas'):
        return resolver(format_name)
    return get_pandas(format_name)


def _resolve_pyreadr_dependency(
    handler: object,
    *,
    format_name: str,
) -> Any:
    """
    Resolve pyreadr, preferring the concrete module resolver when present.

    Parameters
    ----------
    handler : object
        The handler instance to resolve the pyreadr dependency for.
    format_name : str
        The name of the format to resolve pyreadr for.

    Returns
    -------
    Any
        The resolved pyreadr module.
    """
    if resolver := _resolve_module_callable(handler, 'get_dependency'):
        return resolver('pyreadr', format_name=format_name)
    return get_dependency('pyreadr', format_name=format_name)


# SECTION: CLASSES ========================================================== #


class RDataHandlerMixin:
    """
    Shared dependency and payload helpers for R-data scientific handlers.
    """

    # -- Class Attributes -- #

    format_name: ClassVar[str]
    dataset_key: ClassVar[str]

    # -- Instance Methods -- #

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
        records = normalize_records(data, self.format_name)
        return self.dataframe_from_records(records), len(records)

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
        return self.resolve_pandas().DataFrame.from_records(records)

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
        result = self.resolve_pyreadr().read_r(str(path))
        return cast(Mapping[str, object], result)

    def resolve_pandas(self) -> Any:
        """
        Return pandas using shared dependency resolution.

        Returns
        -------
        Any
            The pandas module.
        """
        return _resolve_pandas_dependency(self, format_name=self.format_name)

    def resolve_pyreadr(self) -> Any:
        """
        Return pyreadr using shared dependency resolution.

        Returns
        -------
        Any
            The pyreadr module.
        """
        return _resolve_pyreadr_dependency(self, format_name=self.format_name)
