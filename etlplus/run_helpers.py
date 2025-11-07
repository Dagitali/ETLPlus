"""
etlplus.run_helpers
===================

Helper functions and small utilities used by ``etlplus.run`` to compose API
request/load environments, pagination configs, session objects, and endpoint
clients. Extracted to keep ``run.py`` focused on orchestration while enabling
reuse and testability.

Public (re-export safe) helpers:
- build_pagination_cfg(pagination, overrides)
- build_session(cfg)
- compose_api_request_env(cfg, source_obj, extract_opts)
- compose_api_target_env(cfg, target_obj, overrides)
- build_endpoint_client(base_url, base_path, endpoints, env)
- compute_rl_sleep_seconds(rate_limit, overrides)
- paginate_with_client(client, endpoint_key, params, headers,
  timeout, pagination, sleep_seconds)

Notes
-----
These helpers intentionally accept permissive ``Any``/``Mapping`` inputs to
avoid tight coupling with config dataclasses while keeping runtime flexible.
"""
from __future__ import annotations

import inspect
from typing import Any
from typing import cast
from typing import Mapping
from typing import TypedDict

import requests  # type: ignore

from .api import compute_sleep_seconds
from .api import EndpointClient
from .api import PaginationConfig as ApiPaginationConfig
from .api.types import RetryPolicy as ApiRetryPolicy
from .config.api import ApiConfig as CfgApiConfig
from .config.api import EndpointConfig as CfgEndpointConfig
from .config.pagination import PaginationConfig as CfgPaginationConfig
from .config.rate_limit import RateLimitConfig as CfgRateLimitConfig
from .types import Timeout


# SECTION: EXPORTS ========================================================== #


__all__ = [
    'build_pagination_cfg',
    'build_session',
    'compose_api_request_env',
    'compose_api_target_env',
    'build_endpoint_client',
    'compute_rl_sleep_seconds',
    'paginate_with_client',
    'SessionConfig',
    'ApiRequestEnv',
    'ApiTargetEnv',
]


# SECTION: TYPES ============================================================ #


type Headers = Mapping[str, str]
type Params = Mapping[str, Any]
type URL = str


# SECTION: TYPED DICTS ====================================================== #


class ApiRequestEnv(TypedDict, total=False):
    url: URL | None
    headers: dict[str, str]
    timeout: Timeout
    session: requests.Session | None
    use_endpoints: bool
    base_url: str | None
    base_path: str | None
    endpoints_map: dict[str, str] | None
    endpoint_key: str | None
    params: dict[str, Any]
    pagination: ApiPaginationConfig | None
    sleep_seconds: float
    retry: ApiRetryPolicy | None
    retry_network_errors: bool


class ApiTargetEnv(TypedDict, total=False):
    url: URL | None
    headers: dict[str, str]
    timeout: Timeout
    session: requests.Session | None
    method: str | None


class SessionConfig(TypedDict, total=False):
    headers: Mapping[str, Any]
    params: Mapping[str, Any]
    auth: Any
    verify: bool | str
    cert: Any
    proxies: Mapping[str, Any]
    cookies: Mapping[str, Any]
    trust_env: bool


# SECTION: PROTECTED FUNCTIONS ============================================== #


# -- API Environment Composition -- #


def _get_api_cfg_and_endpoint(
    cfg: Any,
    api_name: str,
    endpoint_name: str,
) -> tuple[CfgApiConfig, CfgEndpointConfig]:
    api_cfg = cfg.apis.get(api_name)
    if not api_cfg:
        raise ValueError(f'API not defined: {api_name}')
    ep = api_cfg.endpoints.get(endpoint_name)
    if not ep:
        raise ValueError(
            f'Endpoint "{endpoint_name}" not defined in API "{api_name}"',
        )
    return api_cfg, ep


def _inherit_http_from_api_endpoint(
    api_cfg: CfgApiConfig,
    ep: CfgEndpointConfig,
    url: URL | None,
    headers: dict[str, str],
    session_cfg: SessionConfig | None,
    force_url: bool = False,
) -> tuple[URL | None, dict[str, str], SessionConfig | None]:
    if force_url or not url:
        url = api_cfg.build_endpoint_url(ep)
    headers = {**api_cfg.headers, **headers}
    session_cfg = _merge_session_cfg_three(api_cfg, ep, session_cfg)
    return url, headers, session_cfg


def _merge_session_cfg_three(
    api_cfg: CfgApiConfig,
    ep: CfgEndpointConfig,
    source_session_cfg: SessionConfig | None,
) -> SessionConfig | None:
    api_sess = getattr(api_cfg, 'session', None)
    ep_sess = getattr(ep, 'session', None)
    merged: dict[str, Any] = {}
    if isinstance(api_sess, dict):
        merged.update(api_sess)
    if isinstance(ep_sess, dict):
        merged.update(ep_sess)
    if isinstance(source_session_cfg, dict):
        merged.update(source_session_cfg)
    return cast(SessionConfig | None, (merged or None))


# SECTION: FUNCTIONS ======================================================== #


# -- API Environment Composition -- #


def build_endpoint_client(
    *,
    base_url: str,
    base_path: str | None,
    endpoints: dict[str, str],
    env: Mapping[str, Any],
) -> EndpointClient:
    # Allow tests to monkeypatch etlplus.run.EndpointClient and have it
    # propagate here by preferring the class on the run module if present.
    try:
        from . import run as run_mod  # local import to avoid cycles
        ClientClass = getattr(run_mod, 'EndpointClient', EndpointClient)
    except Exception:  # pragma: no cover - fallback path
        ClientClass = EndpointClient
    return ClientClass(
        base_url=base_url,
        base_path=base_path,
        endpoints=endpoints,
        retry=env.get('retry'),
        retry_network_errors=bool(env.get('retry_network_errors', False)),
        session=env.get('session'),
    )


def compose_api_request_env(
    cfg: Any,
    source_obj: Any,
    ex_opts: Mapping[str, Any] | None,
) -> ApiRequestEnv:
    ex_opts = ex_opts or {}
    url: URL | None = getattr(source_obj, 'url', None)
    params: dict[str, Any] = dict(
        getattr(source_obj, 'query_params', {}) or {},
    )
    headers: dict[str, str] = dict(
        getattr(source_obj, 'headers', {}) or {},
    )
    pagination = getattr(source_obj, 'pagination', None)
    rate_limit = getattr(source_obj, 'rate_limit', None)
    retry: ApiRetryPolicy | None = cast(
        ApiRetryPolicy | None, getattr(source_obj, 'retry', None),
    )
    retry_network_errors = bool(
        getattr(source_obj, 'retry_network_errors', False),
    )
    session_cfg = cast(
        SessionConfig | None, getattr(source_obj, 'session', None),
    )
    api_name = getattr(source_obj, 'api', None)
    endpoint_name = getattr(source_obj, 'endpoint', None)
    use_client_endpoints = False
    client_base_url: str | None = None
    client_base_path: str | None = None
    client_endpoints_map: dict[str, str] | None = None
    selected_endpoint_key: str | None = None
    if api_name and endpoint_name:
        api_cfg, ep = _get_api_cfg_and_endpoint(cfg, api_name, endpoint_name)
        url, headers, session_cfg = _inherit_http_from_api_endpoint(
            api_cfg, ep, url, headers, session_cfg, force_url=True,
        )
        params = ep.query_params | params
        pagination = (
            pagination
            or ep.pagination
            or api_cfg.effective_pagination_defaults()
        )
        rate_limit = (
            rate_limit
            or ep.rate_limit
            or api_cfg.effective_rate_limit_defaults()
        )
        retry = cast(
            ApiRetryPolicy | None,
            (
                retry
                or getattr(ep, 'retry', None)
                or getattr(api_cfg, 'retry', None)
            ),
        )
        retry_network_errors = (
            retry_network_errors
            or bool(getattr(ep, 'retry_network_errors', False))
            or bool(getattr(api_cfg, 'retry_network_errors', False))
        )
        use_client_endpoints = True
        client_base_url = api_cfg.base_url
        client_base_path = api_cfg.effective_base_path()
        client_endpoints_map = {
            k: v.path
            for k, v in api_cfg.endpoints.items()
        }
        selected_endpoint_key = endpoint_name
    params |= ex_opts.get('query_params', {})
    headers |= ex_opts.get('headers', {})
    timeout: Timeout = ex_opts.get('timeout')
    pag_ov = ex_opts.get('pagination', {})
    rl_ov = ex_opts.get('rate_limit', {})
    rty_ov: ApiRetryPolicy | None = cast(
        ApiRetryPolicy | None,
        (ex_opts.get('retry') if 'retry' in ex_opts else None),
    )
    rne_ov = (
        ex_opts.get('retry_network_errors')
        if 'retry_network_errors' in ex_opts
        else None
    )
    sess_ov: SessionConfig = cast(SessionConfig, ex_opts.get('session', {}))
    sleep_s = compute_rl_sleep_seconds(rate_limit, rl_ov) or 0.0
    if rty_ov is not None:
        retry = rty_ov
    if rne_ov is not None:
        retry_network_errors = bool(rne_ov)
    if isinstance(sess_ov, dict):
        base_cfg: dict[str, Any] = dict(cast(dict, session_cfg or {}))
        base_cfg.update(sess_ov)
        session_cfg = cast(SessionConfig, base_cfg)
    pag_cfg: ApiPaginationConfig | None = build_pagination_cfg(
        pagination,
        pag_ov,
    )
    sess_obj = (
        build_session(session_cfg) if isinstance(session_cfg, dict) else None
    )
    return {
        'use_endpoints': use_client_endpoints,
        'base_url': client_base_url,
        'base_path': client_base_path,
        'endpoints_map': client_endpoints_map,
        'endpoint_key': selected_endpoint_key,
        'url': url,
        'params': params,
        'headers': headers,
        'timeout': timeout,
        'pagination': pag_cfg,
        'sleep_seconds': sleep_s,
        'retry': retry,
        'retry_network_errors': retry_network_errors,
        'session': sess_obj,
    }


def compose_api_target_env(
    cfg: Any,
    target_obj: Any,
    overrides: Mapping[str, Any] | None,
) -> ApiTargetEnv:
    ov = overrides or {}
    url: URL | None = cast(
        URL | None,
        ov.get('url') or getattr(target_obj, 'url', None),
    )
    method: str | None = cast(
        str | None,
        ov.get('method') or getattr(target_obj, 'method', 'post'),
    )
    headers: dict[str, str] = (
        (getattr(target_obj, 'headers', {}) or {})
        | cast(dict[str, str], ov.get('headers', {}))
    )
    timeout: Timeout = (
        cast(Timeout, ov.get('timeout')) if 'timeout' in ov else None
    )
    sess_cfg: SessionConfig | None = cast(
        SessionConfig | None,
        ov.get('session'),
    )
    api_name = getattr(target_obj, 'api', None)
    endpoint_name = getattr(target_obj, 'endpoint', None)
    if api_name and endpoint_name and not url:
        api_cfg, ep = _get_api_cfg_and_endpoint(cfg, api_name, endpoint_name)
        url, headers, sess_cfg = _inherit_http_from_api_endpoint(
            api_cfg,
            ep,
            url,
            headers,
            sess_cfg,
            force_url=False,
        )
    sess_obj = build_session(sess_cfg) if isinstance(sess_cfg, dict) else None

    return {
        'url': url,
        'method': method,
        'headers': headers,
        'timeout': timeout,
        'session': sess_obj,
    }


# -- Pagination -- #


def build_pagination_cfg(
    pagination: CfgPaginationConfig | None,
    overrides: Mapping[str, Any] | None,
) -> ApiPaginationConfig | None:
    ptype: str | None = None
    records_path = None
    max_pages = None
    max_records = None
    if pagination:
        ptype = (getattr(pagination, 'type', '') or '').strip().lower()
        records_path = getattr(pagination, 'records_path', None)
        max_pages = getattr(pagination, 'max_pages', None)
        max_records = getattr(pagination, 'max_records', None)
    if overrides:
        ptype = (overrides.get('type') or ptype or '').strip().lower()
        records_path = overrides.get('records_path', records_path)
        max_pages = overrides.get('max_pages', max_pages)
        max_records = overrides.get('max_records', max_records)
    if not ptype:
        return None
    cfg: dict[str, Any] = {
        'type': ptype,
        'records_path': records_path,
        'max_pages': max_pages,
        'max_records': max_records,
    }
    match ptype:
        case 'page' | 'offset':
            page_param = overrides.get('page_param') if overrides else None
            size_param = overrides.get('size_param') if overrides else None
            start_page = overrides.get('start_page') if overrides else None
            page_size = overrides.get('page_size') if overrides else None
            if pagination:
                page_param = (
                    page_param
                    or getattr(pagination, 'page_param', None)
                    or 'page'
                )
                size_param = (
                    size_param
                    or getattr(pagination, 'size_param', None)
                    or 'per_page'
                )
                start_page = (
                    start_page
                    or getattr(pagination, 'start_page', None)
                    or 1
                )
                page_size = (
                    page_size
                    or getattr(pagination, 'page_size', None)
                    or 100
                )
            cfg.update({
                'page_param': str(page_param or 'page'),
                'size_param': str(size_param or 'per_page'),
                'start_page': int(start_page or 1),
                'page_size': int(page_size or 100),
            })
        case 'cursor':
            cursor_param = (
                overrides.get('cursor_param') if overrides else None
            )
            cursor_path = (
                overrides.get('cursor_path') if overrides else None
            )
            page_size = (
                overrides.get('page_size') if overrides else None
            )
            start_cursor = None
            if pagination:
                cursor_param = (
                    cursor_param
                    or getattr(pagination, 'cursor_param', None)
                    or 'cursor'
                )
                cursor_path = (
                    cursor_path
                    or getattr(pagination, 'cursor_path', None)
                )
                page_size = (
                    page_size
                    or getattr(pagination, 'page_size', None)
                    or 100
                )
                start_cursor = getattr(pagination, 'start_cursor', None)
            cfg.update({
                'cursor_param': str(cursor_param or 'cursor'),
                'cursor_path': cursor_path,
                'page_size': int(page_size or 100),
                'start_cursor': start_cursor,
            })
        case _:
            pass

    return cast(ApiPaginationConfig, cfg)


# -- Pagination Invocation -- #


def paginate_with_client(
    client: Any,
    endpoint_key: str,
    params: Params | None,
    headers: Headers | None,
    timeout: Timeout,
    pagination: ApiPaginationConfig | None,
    sleep_seconds: float | None,
) -> Any:
    sig = inspect.signature(client.paginate)  # type: ignore[arg-type]
    kw_pag: dict[str, Any] = {'pagination': pagination}
    if '_params' in sig.parameters:
        kw_pag['_params'] = params
    else:
        kw_pag['params'] = params
    if '_headers' in sig.parameters:
        kw_pag['_headers'] = headers
    else:
        kw_pag['headers'] = headers
    if '_timeout' in sig.parameters:
        kw_pag['_timeout'] = timeout
    else:
        kw_pag['timeout'] = timeout
    eff_sleep = 0.0 if sleep_seconds is None else sleep_seconds
    if '_sleep_seconds' in sig.parameters:
        kw_pag['_sleep_seconds'] = eff_sleep
    else:
        kw_pag['sleep_seconds'] = eff_sleep

    return client.paginate(endpoint_key, **kw_pag)


# -- Rate Limit -- #


def compute_rl_sleep_seconds(
    rate_limit: CfgRateLimitConfig | Mapping[str, Any] | None,
    overrides: Mapping[str, Any] | None,
) -> float | None:
    rl_map: Mapping[str, Any] | None
    if rate_limit and hasattr(rate_limit, 'sleep_seconds'):
        rl_map = {
            'sleep_seconds': getattr(rate_limit, 'sleep_seconds', None),
            'max_per_sec': getattr(rate_limit, 'max_per_sec', None),
        }
    else:
        rl_map = cast(Mapping[str, Any] | None, rate_limit)

    return compute_sleep_seconds(cast(Any, rl_map), overrides or {})


# -- Session -- #


def build_session(cfg: SessionConfig | None) -> requests.Session:
    s = requests.Session()
    if not cfg:
        return s
    headers = cfg.get('headers')
    if isinstance(headers, dict):
        s.headers.update(headers)
    params = cfg.get('params')
    if isinstance(params, dict):
        try:
            setattr(s, 'params', params)
        except (AttributeError, TypeError):
            pass
    auth = cfg.get('auth')
    if auth is not None:
        if isinstance(auth, (list, tuple)) and len(auth) == 2:
            s.auth = (auth[0], auth[1])  # type: ignore[assignment]
        else:
            s.auth = auth  # type: ignore[assignment]
    if 'verify' in cfg:
        s.verify = cfg.get('verify')  # type: ignore[assignment]
    cert = cfg.get('cert')
    if cert is not None:
        s.cert = cert  # type: ignore[assignment]
    proxies = cfg.get('proxies')
    if isinstance(proxies, dict):
        s.proxies.update(proxies)
    cookies = cfg.get('cookies')
    if isinstance(cookies, dict):
        try:
            s.cookies.update(cookies)
        except (TypeError, ValueError):
            pass
    if 'trust_env' in cfg:
        try:
            # type: ignore[attr-defined]
            s.trust_env = bool(cfg.get('trust_env'))
        except AttributeError:
            pass

    return s
