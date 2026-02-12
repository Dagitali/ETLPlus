"""
:mod:`etlplus.file._r` module.

Shared helpers for R-related file formats.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..types import JSONData
from ..types import JSONDict
from ._io import records_from_table

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'coerce_r_object',
    'coerce_r_result',
    'list_r_dataset_keys',
]


# SECTION: FUNCTIONS ======================================================== #


def coerce_r_object(value: Any, pandas: Any) -> JSONData:
    """
    Normalize a pyreadr object into JSON-friendly data.

    Parameters
    ----------
    value : Any
        Object returned by ``pyreadr``.
    pandas : Any
        pandas module used for DataFrame checks.

    Returns
    -------
    JSONData
        Normalized JSON-like payload.
    """
    if isinstance(value, pandas.DataFrame):
        return records_from_table(value)
    if isinstance(value, dict):
        return value
    if isinstance(value, list) and all(
        isinstance(item, dict) for item in value
    ):
        return value
    return {'value': value}


def coerce_r_result(
    result: Mapping[str, object],
    *,
    dataset: str | None,
    dataset_key: str,
    format_name: str,
    pandas: Any,
) -> JSONData:
    """
    Resolve and normalize an R result mapping into JSON-like data.

    Parameters
    ----------
    result : Mapping[str, object]
        Result mapping returned by ``pyreadr.read_r``.
    dataset : str | None
        Optional dataset key to select.
    dataset_key : str
        Default dataset key for single-dataset aliases.
    format_name : str
        Human-readable format name for error messages.
    pandas : Any
        ``pandas`` module used for DataFrame checks.

    Returns
    -------
    JSONData
        Normalized dataset payload.

    Raises
    ------
    ValueError
        If an explicit dataset key is requested but not found.
    """
    if not result:
        return []

    if dataset is not None:
        if dataset in result:
            return coerce_r_object(result[dataset], pandas)
        if dataset == dataset_key and len(result) == 1:
            return coerce_r_object(next(iter(result.values())), pandas)
        raise ValueError(f'{format_name} dataset {dataset!r} not found')

    if len(result) == 1:
        return coerce_r_object(next(iter(result.values())), pandas)

    payload: JSONDict = {}
    for key, value in result.items():
        payload[str(key)] = coerce_r_object(value, pandas)
    return payload


def list_r_dataset_keys(
    result: Mapping[str, object],
    *,
    default_key: str,
) -> list[str]:
    """
    List dataset/object keys from a ``pyreadr`` result mapping.

    Parameters
    ----------
    result : Mapping[str, object]
        Result mapping returned by ``pyreadr.read_r``.
    default_key : str
        Fallback key when no objects exist in the container.

    Returns
    -------
    list[str]
        Available dataset keys.
    """
    if not result:
        return [default_key]
    return [str(key) for key in result]
