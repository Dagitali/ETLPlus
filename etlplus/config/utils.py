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

from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any
from typing import cast
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


# SECTION: TYPE ALIASES ===================================================== #


type StrAnyMap = Mapping[str, Any]


# SECTION: FUNCTIONS ======================================================== #


def cast_str_dict(
    mapping: StrAnyMap | None,
) -> dict[str, str]:
    """
    Return a new ``dict`` with all values coerced to ``str``.

    This is commonly used for HTTP headers where values can be numbers or
    other primitives but need to be serialized as strings.

    Parameters
    ----------
    mapping : StrAnyMap | None
        Mapping whose values are stringified; ``None`` yields ``{}``.

    Returns
    -------
    dict[str, str]
        Dictionary of the original key/value pairs converted via ``str()``.
    """
    if not mapping:
        return {}
    return {str(key): str(value) for key, value in mapping.items()}


def deep_substitute(
    value: Any,
    vars_map: StrAnyMap | None,
    env_map: Mapping[str, str] | None,
) -> Any:
    """
    Recursively substitute ``${VAR}`` tokens in nested structures.

    Only strings are substituted; other types are returned as-is.

    Parameters
    ----------
    value : Any
        The value to perform substitutions on.
    vars_map : StrAnyMap | None
        Mapping of variable names to replacement values (lower precedence).
    env_map : Mapping[str, str] | None
        Mapping of environment variable overriding ``vars_map`` values (higher
        precedence).

    Returns
    -------
    Any
        New structure with substitutions applied where tokens were found.
    """
    substitutions = _prepare_substitutions(vars_map, env_map)

    def _apply(node: Any) -> Any:
        match node:
            case str():
                return _replace_tokens(node, substitutions)
            case Mapping():
                return {k: _apply(v) for k, v in node.items()}
            case list() | tuple() as seq:
                apply = [_apply(item) for item in seq]
                return apply if isinstance(seq, list) else tuple(apply)
            case _:
                return node

    return _apply(value)


def pagination_from_defaults(
    obj: StrAnyMap | None,
) -> PaginationConfig | None:
    """
    Extract pagination type and integer bounds from defaults mapping.

    Tolerates either a flat PaginationConfig-like mapping or a nested shape
    with "params" and "response" blocks. Unknown keys are ignored.

    Parameters
    ----------
    obj : StrAnyMap | None
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
    from .pagination import PaginationConfig
    # from .types import PaginationType
    from ..api import PaginationType

    # Normalize pagination type to supported literal when possible.
    norm_type: PaginationType | None
    match str(ptype).strip().lower() if ptype is not None else '':
        case 'page':
            norm_type = PaginationType.PAGE  # 'page'
        case 'offset':
            norm_type = PaginationType.OFFSET  # 'offset'
        case 'cursor':
            norm_type = PaginationType.CURSOR  # 'cursor'
        case _:
            norm_type = None

    return PaginationConfig(
        type=cast(PaginationType, norm_type),
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
    obj: StrAnyMap | None,
) -> RateLimitConfig | None:
    """
    Return numeric rate-limit bounds from defaults mapping.

    Only supports sleep_seconds and max_per_sec. Other keys are ignored.

    Parameters
    ----------
    obj : StrAnyMap | None
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


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _prepare_substitutions(
    vars_map: StrAnyMap | None,
    env_map: Mapping[str, Any] | None,
) -> tuple[tuple[str, Any], ...]:
    if not vars_map and not env_map:
        return ()
    merged: dict[str, Any] = {}
    if vars_map:
        merged.update(vars_map)
    if env_map:
        merged.update(env_map)
    return tuple(merged.items())


def _replace_tokens(
    text: str,
    substitutions: Iterable[tuple[str, Any]],
) -> str:
    if not substitutions:
        return text
    out = text
    for name, replacement in substitutions:
        token = f"${{{name}}}"
        if token in out:
            out = out.replace(token, str(replacement))
    return out


def _mapping_or_none(value: Any) -> StrAnyMap | None:
    return value if isinstance(value, Mapping) else None
