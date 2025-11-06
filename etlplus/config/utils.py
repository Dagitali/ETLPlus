"""
etlplus.config.utils
====================

A module defining utility functions for ETL pipeline configuration.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .pagination import PaginationConfig
    from .rate_limit import RateLimitConfig


# SECTION: EXPORTS ========================================================== #


__all__ = [
    'cast_str_dict',
    'deep_substitute',
    'pagination_from_defaults',
    'rate_limit_from_defaults',
    'to_int',
    'to_float',
]


# SECTION: FUNCTIONS ======================================================== #


def cast_str_dict(
    m: Mapping[str, Any] | None,
) -> dict[str, str]:
    """
    Return a shallow copy of a mapping with all values coerced to strings.

    This is commonly used for HTTP headers where values can be numbers or
    other primitives but need to be serialized as strings.

    Parameters
    ----------
    m : Mapping[str, Any] | None
        The mapping to coerce.

    Returns
    -------
    dict[str, str]
        A new mapping with all values as strings.
    """

    return {k: str(v) for k, v in (m or {}).items()}


def deep_substitute(
    value: Any,
    vars_map: Mapping[str, Any],
    env_map: Mapping[str, str],
) -> Any:
    """
    Recursively substitute ${VAR} tokens using vars and environment.

    Only strings are substituted; other types are returned as-is.

    Parameters
    ----------
    value : Any
        The value to perform substitutions on.
    vars_map : Mapping[str, Any]
        A mapping of variable names to replacement values.
    env_map : Mapping[str, str]
        A mapping of environment variable names to replacement values.

    Returns
    -------
    Any
        The value with substitutions applied.
    """

    if isinstance(value, str):
        # Fast path: single combined pass over substitutions.
        if not vars_map and not env_map:
            return value
        out = value
        # Merge mappings using dict union for a single pass over replacements.
        all_vars: dict[str, Any] = (
            dict(vars_map) | dict(env_map)
            if env_map else dict(vars_map)
        )
        for name, replacement in all_vars.items():
            out = out.replace(f"${{{name}}}", str(replacement))
        return out
    if isinstance(value, dict):
        return {
            k: deep_substitute(v, vars_map, env_map)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [deep_substitute(v, vars_map, env_map) for v in value]

    return value


def pagination_from_defaults(
    obj: Mapping[str, Any] | None,
) -> PaginationConfig | None:
    """
    Best-effort parser for profile-level defaults.pagination structures.

    Tolerates either a flat PaginationConfig-like mapping or a nested shape
    with "params" and "response" blocks. Unknown keys are ignored.

    Parameters
    ----------
    obj : Mapping[str, Any] | None
        The object to parse (expected to be a mapping).

    Returns
    -------
    PaginationConfig | None
        A PaginationConfig instance, or None if parsing failed.
    """

    if not isinstance(obj, Mapping):
        return None

    # Start with direct keys if present.
    ptype = obj.get('type')
    page_param = obj.get('page_param')
    size_param = obj.get('size_param')
    start_page = obj.get('start_page')
    page_size = obj.get('page_size')
    cursor_param = obj.get('cursor_param')
    cursor_path = obj.get('cursor_path')
    records_path = obj.get('records_path')
    max_pages = obj.get('max_pages')
    max_records = obj.get('max_records')

    # Map from nested shapes when provided.
    params_val = obj.get('params')
    params_blk = params_val if isinstance(params_val, Mapping) else None
    if params_blk and not page_param:
        page_param = params_blk.get('page')
    if params_blk and not size_param:
        size_param = params_blk.get('per_page') or params_blk.get('limit')
    if params_blk and not cursor_param:
        cursor_param = params_blk.get('cursor')

    resp_val = obj.get('response')
    resp_blk = resp_val if isinstance(resp_val, Mapping) else None
    if resp_blk and not records_path:
        records_path = resp_blk.get('items_path')
    if resp_blk and not cursor_path:
        cursor_path = resp_blk.get('next_cursor_path')

    dflt_val = obj.get('defaults')
    dflt_blk = dflt_val if isinstance(dflt_val, Mapping) else None
    if dflt_blk and not page_size:
        page_size = dflt_blk.get('per_page')

    # Local import to avoid circular dependency with pagination -> utils
    from .pagination import PaginationConfig as _PaginationConfig

    return _PaginationConfig(
        type=str(ptype) if ptype is not None else None,
        page_param=page_param,
        size_param=size_param,
        start_page=start_page,
        page_size=page_size,
        cursor_param=cursor_param,
        cursor_path=cursor_path,
        start_cursor=None,
        records_path=records_path,
        max_pages=max_pages,
        max_records=max_records,
    )


def rate_limit_from_defaults(
    obj: Mapping[str, Any] | None,
) -> RateLimitConfig | None:
    """
    Best-effort parser for profile-level defaults.rate_limit structures.

    Only supports sleep_seconds and max_per_sec. Other keys are ignored.

    Parameters
    ----------
    obj : Mapping[str, Any] | None
        The object to parse (expected to be a mapping).

    Returns
    -------
    RateLimitConfig | None
        A RateLimitConfig instance, or None if parsing failed.
    """

    if not isinstance(obj, Mapping):
        return None
    sleep_seconds = obj.get('sleep_seconds')
    max_per_sec = obj.get('max_per_sec')
    if sleep_seconds is None and max_per_sec is None:
        return None
    # Local import to avoid circular dependency with rate_limit -> utils
    from .rate_limit import RateLimitConfig as _RateLimitConfig

    return _RateLimitConfig(
        sleep_seconds=sleep_seconds,
        max_per_sec=max_per_sec,
    )


# Small numeric coercion helpers used by config parsers. These are
# intentionally forgiving: non-coercible values return None.

def to_int(
    v: Any,
) -> int | None:
    """
    Coerce a value to an integer.

    Parameters
    ----------
    v : Any
        The value to coerce.

    Returns
    -------
    int | None
        The coerced integer value, or None if coercion failed.
    """
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def to_float(
    v: Any,
) -> float | None:
    """
    Coerce a value to a float.

    Parameters
    ----------
    v : Any
        The value to coerce.

    Returns
    -------
    float | None
        The coerced float value, or None if coercion failed.
    """

    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None
