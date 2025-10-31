"""
ETLPlus API Pagination
======================

REST API helpers for pagination.
"""
from __future__ import annotations

from typing import Any

from ..extract import extract as _extract
from .request import apply_sleep
from .request import build_request_kwargs


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _coalesce_records(x: Any, records_path: str | None) -> list[dict]:
    """
    Coalesce JSON page payloads into a list of dicts.

    Supports dotted path extraction via ``records_path`` and handles lists,
    maps, and scalars by coercing non-dict items into ``{'value': x}``.

    Parameters
    ----------
    x : Any
        The JSON payload from an API response.
    records_path : str | None
        Dotted path to the records within the payload.

    Returns
    -------
    list[dict]
        List of record dicts extracted from the payload.
    """
    def _get_path(obj: Any, path: str) -> Any:
        cur = obj
        for part in path.split('.'):
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                return None
        return cur

    data = x
    if isinstance(records_path, str) and records_path:
        data = _get_path(x, records_path)

    if isinstance(data, list):
        out: list[dict] = []
        for item in data:
            if isinstance(item, dict):
                out.append(item)
            else:
                out.append({'value': item})
        return out
    if isinstance(data, dict):
        items = data.get('items')
        if isinstance(items, list):
            return _coalesce_records(items, None)
        return [data]
    return [{'value': data}]


# SECTION: FUNCTIONS ======================================================== #


def paginate(
    url: str,
    params: dict[str, Any] | None,
    headers: dict[str, Any] | None,
    timeout: float | int | None,
    pagination: dict[str, Any] | None,
    *,
    sleep_seconds: float = 0.0,
) -> Any:
    """
    Paginate API responses and aggregate records.

    The ``pagination`` dict supports:
      - type: 'page'|'offset'|'cursor' (or falsy for single request)
      - records_path, max_pages, max_records
      - For 'page'/'offset': page_param, size_param, start_page, page_size
      - For 'cursor': cursor_param, cursor_path, start_cursor, page_size

    Returns raw payload for non-paginated calls; lists for paginated.
    """
    # Single call when pagination type is missing/empty
    ptype = (pagination or {}).get('type') if pagination else None
    if not ptype:
        kw = build_request_kwargs(
            params=params, headers=headers, timeout=timeout,
        )
        return _extract('api', url, **kw)

    records_path = (pagination or {}).get('records_path')
    max_pages = (pagination or {}).get('max_pages')
    max_records = (pagination or {}).get('max_records')

    def _stop_limits(pages: int, recs: int) -> bool:
        if isinstance(max_pages, int) and pages >= max_pages:
            return True
        if isinstance(max_records, int) and recs >= max_records:
            return True
        return False

    results: list[dict] = []
    pages = 0
    recs = 0

    # Page/offset strategy
    if ptype in {'page', 'offset'}:
        page_param = (pagination or {}).get('page_param') or 'page'
        size_param = (pagination or {}).get('size_param') or 'per_page'
        start_page = int((pagination or {}).get('start_page') or 1)
        page_size = int((pagination or {}).get('page_size') or 100)

        current = start_page
        while True:
            req_params = dict(params or {})
            req_params[str(page_param)] = current
            req_params[str(size_param)] = page_size
            kw = build_request_kwargs(
                params=req_params, headers=headers, timeout=timeout,
            )
            page_data = _extract('api', url, **kw)
            batch = _coalesce_records(page_data, records_path)
            results.extend(batch)
            n = len(batch)
            pages += 1
            recs += n
            # Stop if short page
            if n < page_size:
                break
            # Stop on limits
            if _stop_limits(pages, recs):
                if isinstance(max_records, int):
                    results[:] = results[: int(max_records)]
                break
            current += 1
            apply_sleep(sleep_seconds)
        return results

    # Cursor strategy
    if ptype == 'cursor':
        cursor_param = (pagination or {}).get('cursor_param') or 'cursor'
        cursor_path = (pagination or {}).get('cursor_path')
        page_size = int((pagination or {}).get('page_size') or 100)
        cursor_value = (pagination or {}).get('start_cursor')

        def _next_cursor_from(
            data_obj: Any, path: str | None,
        ) -> Any:
            if not (
                isinstance(path, str) and path and isinstance(data_obj, dict)
            ):
                return None
            cur: Any = data_obj
            for part in path.split('.'):  # dotted path
                if isinstance(cur, dict):
                    cur = cur.get(part)
                else:
                    return None
            return cur if isinstance(cur, (str, int)) else None

        while True:
            req_params = dict(params or {})
            if cursor_value is not None:
                req_params[str(cursor_param)] = cursor_value
            if page_size:
                req_params.setdefault('limit', page_size)
            kw = build_request_kwargs(
                params=req_params, headers=headers, timeout=timeout,
            )
            page_data = _extract('api', url, **kw)
            batch = _coalesce_records(page_data, records_path)
            results.extend(batch)
            n = len(batch)
            pages += 1
            recs += n

            nxt = _next_cursor_from(page_data, cursor_path)
            if not nxt or n == 0:
                break
            if _stop_limits(pages, recs):
                if isinstance(max_records, int):
                    results[:] = results[: int(max_records)]
                break
            cursor_value = nxt
            apply_sleep(sleep_seconds)
        return results

    # Unknown pagination type -> single request
    kw = build_request_kwargs(
        params=params, headers=headers, timeout=timeout,
    )
    return _extract('api', url, **kw)
