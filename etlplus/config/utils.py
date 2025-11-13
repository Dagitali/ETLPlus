"""
etlplus.config.utils module.

A module defining utility helpers for ETL pipeline configuration.

Notes
-----
- Inputs to parsers favor ``Mapping[str, Any]`` to remain permissive and
    avoid unnecessary copies; normalization returns concrete types.
- Substitution is shallow for strings and recursive for containers.
- Numeric coercion helpers are intentionally forgiving: invalid values
    become ``None`` rather than raising.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import cast
from typing import Iterable
from typing import TYPE_CHECKING

from ..utils import to_float
from ..utils import to_int

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
    Return a new ``dict`` with all values coerced to ``str``.

    This is commonly used for HTTP headers where values can be numbers or
    other primitives but need to be serialized as strings.

    Parameters
    ----------
    m : Mapping[str, Any] | None
        The mapping to coerce.

    Returns
    -------
    dict[str, str]
        Dictionary of the original pairs converted via ``str()``.
    """
    return {k: str(v) for k, v in (m or {}).items()}


def deep_substitute(
    value: Any,
    vars_map: Mapping[str, Any],
    env_map: Mapping[str, str],
) -> Any:
    """
    Recursively substitute ``${VAR}`` tokens in nested structures.

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
        New structure with substitutions applied where tokens were found.
    """
    if isinstance(value, str):
        # Fast path: single combined pass over substitutions.
        if not (vars_map or env_map):
            return value

        # Union preserves right-hand precedence (env overrides vars).
        merged: Iterable[tuple[str, Any]] = (
            (dict(vars_map) | dict(env_map)).items()
            if env_map else vars_map.items()
        )
        out = value
        for name, replacement in merged:
            token = f"${{{name}}}"
            if token in out:
                out = out.replace(token, str(replacement))
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
    Extract pagination type and integer bounds from defaults mapping.

    Tolerates either a flat PaginationConfig-like mapping or a nested shape
    with "params" and "response" blocks. Unknown keys are ignored.

    Parameters
    ----------
    obj : Mapping[str, Any] | None
        The object to parse (expected to be a mapping).

    Returns
    -------
    PaginationConfig | None
        A PaginationConfig instance with numeric fields coerced to int/float
        where applicable, or None if parsing failed.
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

    # Locally import inside function to avoid circular dependencies; narrow to
    # literal.
    from .pagination import PaginationConfig as _PaginationConfig
    from .types import PaginationType as _PaginationType

    # Normalize pagination type to supported literal when possible.
    norm_type: _PaginationType | None
    match str(ptype).strip().lower() if ptype is not None else '':
        case 'page':
            norm_type = cast(_PaginationType, 'page')
        case 'offset':
            norm_type = cast(_PaginationType, 'offset')
        case 'cursor':
            norm_type = cast(_PaginationType, 'cursor')
        case _:
            norm_type = None

    return _PaginationConfig(
        type=norm_type,
        page_param=page_param,
        size_param=size_param,
        start_page=to_int(start_page),
        page_size=to_int(page_size),
        cursor_param=cursor_param,
        cursor_path=cursor_path,
        start_cursor=None,
        records_path=records_path,
        max_pages=to_int(max_pages),
        max_records=to_int(max_records),
    )


def rate_limit_from_defaults(
    obj: Mapping[str, Any] | None,
) -> RateLimitConfig | None:
    """
    Return numeric rate-limit bounds from defaults mapping.

    Only supports sleep_seconds and max_per_sec. Other keys are ignored.

    Parameters
    ----------
    obj : Mapping[str, Any] | None
        The object to parse (expected to be a mapping).

    Returns
    -------
    RateLimitConfig | None
        A RateLimitConfig instance with numeric fields coerced, or None if
        parsing failed.
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
        sleep_seconds=to_float(sleep_seconds),
        max_per_sec=to_float(max_per_sec),
    )
