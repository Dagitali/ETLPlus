"""
:mod:`etlplus.utils._mapping` module.

Mapping-oriented utility helpers.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ._types import StrAnyMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions (mapping utilities)
    'cast_str_dict',
    'coerce_dict',
    'maybe_mapping',
]


# SECTION: FUNCTIONS ======================================================== #


def cast_str_dict(
    mapping: StrAnyMap | None,
) -> dict[str, str]:
    """
    Return a new ``dict`` with keys and values coerced to ``str``.

    Parameters
    ----------
    mapping : StrAnyMap | None
        Mapping to normalize; ``None`` yields ``{}``.

    Returns
    -------
    dict[str, str]
        Dictionary of the original key/value pairs converted via :func:`str`.
    """
    if not mapping:
        return {}
    return {str(key): str(value) for key, value in mapping.items()}


def coerce_dict(
    value: Any,
) -> dict[str, Any]:
    """
    Return a ``dict`` copy when *value* is mapping-like.

    Parameters
    ----------
    value : Any
        Mapping-like object to copy. ``None`` returns an empty dict.

    Returns
    -------
    dict[str, Any]
        Shallow copy of *value* converted to a standard ``dict``.
    """
    return dict(value) if isinstance(value, Mapping) else {}


def maybe_mapping(
    value: Any,
) -> StrAnyMap | None:
    """
    Return *value* when it is mapping-like; otherwise ``None``.

    Parameters
    ----------
    value : Any
        Value to test.

    Returns
    -------
    StrAnyMap | None
        The input value if it is a mapping; ``None`` if not.
    """
    return value if isinstance(value, Mapping) else None
