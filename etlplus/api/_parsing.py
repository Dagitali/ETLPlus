"""
:mod:`etlplus.api._parsing` module.

Shared parsing helpers for :mod:`etlplus.api` modules.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..types import StrAnyMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'cast_str_dict',
    'coerce_dict',
    'maybe_mapping',
]


# SECTION: FUNCTIONS ======================================================== #


def cast_str_dict(
    mapping: StrAnyMap | None,
) -> dict[str, str]:
    """
    Return a ``dict`` with stringified keys and values when possible.

    Parameters
    ----------
    mapping : StrAnyMap | None
        Input mapping; ``None`` yields ``{}``.

    Returns
    -------
    dict[str, str]
        Dictionary with all keys and values converted via :func:`str()`.
    """
    if not mapping:
        return {}
    return {str(key): str(value) for key, value in mapping.items()}


def coerce_dict(
    value: Any,
) -> dict[str, Any]:
    """
    Return a shallow ``dict`` copy when *value* is mapping-like.

    Parameters
    ----------
    value : Any
        Mapping-like object to copy. ``None`` returns an empty dict.

    Returns
    -------
    dict[str, Any]
        Shallow copy of the mapping or empty dict.
    """
    return dict(value) if isinstance(value, Mapping) else {}


def maybe_mapping(
    value: Any,
) -> StrAnyMap | None:
    """
    Return *value* when it behaves like a mapping; otherwise ``None``.

    Return *value* only when it behaves like a mapping; otherwise ``None``.

    Parameters
    ----------
    value : Any
        Value to check.

    Returns
    -------
    StrAnyMap | None
        The original value if mapping-like; otherwise ``None``.
    """
    return value if isinstance(value, Mapping) else None
