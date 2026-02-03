"""
:mod:`etlplus.file._r` module.

Shared helpers for R-related file formats.
"""

from __future__ import annotations

from typing import Any

from ..types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'coerce_r_object',
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
        return value.to_dict(orient='records')
    if isinstance(value, dict):
        return value
    if isinstance(value, list) and all(
        isinstance(item, dict) for item in value
    ):
        return value
    return {'value': value}
