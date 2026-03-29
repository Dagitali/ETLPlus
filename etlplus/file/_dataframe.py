"""
:mod:`etlplus.file._dataframe` module.

Shared helpers for building pandas DataFrame-like tables from records/data.
"""

from __future__ import annotations

from typing import Any

from ..utils._types import JSONData
from ..utils._types import JSONList
from ._io import normalize_records

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'dataframe_and_count_from_data',
    'dataframe_from_data',
    'dataframe_from_records',
]


# SECTION: FUNCTIONS ======================================================== #


def dataframe_from_records(
    pandas: Any,
    records: JSONList,
) -> Any:
    """Build one DataFrame-like table from row records."""
    return pandas.DataFrame.from_records(records)


def dataframe_from_data(
    pandas: Any,
    data: JSONData,
    *,
    format_name: str,
) -> Any:
    """Normalize JSON-like payload and build one DataFrame-like table."""
    return dataframe_from_records(
        pandas,
        normalize_records(data, format_name),
    )


def dataframe_and_count_from_data(
    pandas: Any,
    data: JSONData,
    *,
    format_name: str,
) -> tuple[Any, int]:
    """Normalize JSON-like payload and return one table plus record count."""
    records = normalize_records(data, format_name)
    return dataframe_from_records(pandas, records), len(records)
