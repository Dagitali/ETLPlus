"""
:mod:`etlplus.file._r_handlers` module.

Shared abstractions for R-data handlers.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import ClassVar
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ._imports import FormatPandasResolverMixin
from ._imports import resolve_dependency
from ._io import normalize_records
from ._r import coerce_r_result

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RDataHandlerMixin',
]


# SECTION: CLASSES ========================================================== #


class RDataHandlerMixin(FormatPandasResolverMixin):
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
        return self.dataframe_from_records(
            records := normalize_records(data, self.format_name),
        ), len(records)

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
        return cast(
            Mapping[str, object],
            self.resolve_pyreadr().read_r(str(path)),
        )

    def resolve_pyreadr(self) -> Any:
        """
        Return pyreadr using shared dependency resolution.

        Returns
        -------
        Any
            The pyreadr module.
        """
        return resolve_dependency(
            self,
            'pyreadr',
            format_name=self.format_name,
        )
