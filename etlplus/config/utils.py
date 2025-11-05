"""
etlplus.config.utils
====================

A module defining utility functions for ETL pipeline configuration.
"""
from __future__ import annotations

from typing import Any

from ..types import StrAnyMap
from ..types import StrStrMap
from .pagination import PaginationConfig
from .rate_limit import RateLimitConfig


# SECTION: FUNCTIONS ======================================================== #


def deep_substitute(
    value: Any,
    vars_map: StrAnyMap,
    env_map: StrStrMap,
) -> Any:
    """
    Recursively substitute ${VAR} tokens using vars and environment.

    Only strings are substituted; other types are returned as-is.
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


def pagination_from_defaults(obj: Any) -> PaginationConfig | None:
    """
    Best-effort parser for profile-level defaults.pagination structures.

    Tolerates either a flat PaginationConfig-like mapping or a nested shape
    with "params" and "response" blocks. Unknown keys are ignored.
    """

    if not isinstance(obj, dict):
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
    params_blk = (
        obj.get('params') if isinstance(obj.get('params'), dict) else None
    )
    if params_blk and not page_param:
        page_param = params_blk.get('page')
    if params_blk and not size_param:
        size_param = params_blk.get('per_page') or params_blk.get('limit')
    if params_blk and not cursor_param:
        cursor_param = params_blk.get('cursor')

    resp_blk = (
        obj.get('response') if isinstance(obj.get('response'), dict) else None
    )
    if resp_blk and not records_path:
        records_path = resp_blk.get('items_path')
    if resp_blk and not cursor_path:
        cursor_path = resp_blk.get('next_cursor_path')

    dflt_blk = (
        obj.get('defaults') if isinstance(obj.get('defaults'), dict) else None
    )
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


def rate_limit_from_defaults(obj: Any) -> RateLimitConfig | None:
    """
    Best-effort parser for profile-level defaults.rate_limit structures.

    Only supports sleep_seconds and max_per_sec. Other keys are ignored.
    """

    if not isinstance(obj, dict):
        return None
    sleep_seconds = obj.get('sleep_seconds')
    max_per_sec = obj.get('max_per_sec')
    if sleep_seconds is None and max_per_sec is None:
        return None
    return RateLimitConfig(
        sleep_seconds=sleep_seconds,
        max_per_sec=max_per_sec,
    )
