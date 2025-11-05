"""
etlplus.config.utils
====================

A module defining utility functions for ETL pipeline configuration.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..types import StrAnyMap
from ..types import StrStrMap
from .pagination import PaginationConfig
from .rate_limit import RateLimitConfig
from .types import Config


# SECTION: FUNCTIONS ======================================================== #


def deep_substitute(
    value: Any,
    vars_map: StrAnyMap,
    env_map: StrStrMap,
) -> Any:
    """
    Recursively substitute ${VAR} tokens using vars and environment.

    Only strings are substituted; other types are returned as-is.

    Parameters
    ----------
    value : Any
        The value to perform substitutions on.
    vars_map : StrAnyMap
        A mapping of variable names to replacement values.
    env_map : StrStrMap
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
        all_vars: dict[str, Any] = {}
        if vars_map:
            all_vars.update(vars_map)
        if env_map:
            all_vars.update(env_map)
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
    obj: Config | None,
) -> PaginationConfig | None:
    """
    Best-effort parser for profile-level defaults.pagination structures.

    Tolerates either a flat PaginationConfig-like mapping or a nested shape
    with "params" and "response" blocks. Unknown keys are ignored.

    Parameters
    ----------
    obj : Config | None
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

    return PaginationConfig(
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
    obj: Config | None,
) -> RateLimitConfig | None:
    """
    Best-effort parser for profile-level defaults.rate_limit structures.

    Only supports sleep_seconds and max_per_sec. Other keys are ignored.

    Parameters
    ----------
    obj : Config | None
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
    return RateLimitConfig(
        sleep_seconds=sleep_seconds,
        max_per_sec=max_per_sec,
    )
