"""
etlplus.run
============

A module for running ETL jobs defined in YAML configurations.
"""
from __future__ import annotations

import inspect
from typing import Any
from typing import cast
from typing import Mapping
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

import requests  # type: ignore

from .api import compute_sleep_seconds
from .api import EndpointClient
from .api import PaginationConfig as ApiPaginationConfig
from .config import load_pipeline_config
from .config.pagination import PaginationConfig as CfgPaginationConfig
from .config.rate_limit import RateLimitConfig as CfgRateLimitConfig
from .extract import extract
from .load import load
from .transform import transform
from .types import JSONDict
from .utils import print_json
from .validate import validate
from .validation.utils import maybe_validate


# SECTION: EXPORTS ========================================================== #


__all__ = ['run']


# SECTION: CONSTANTS ======================================================== #


DEFAULT_CONFIG_PATH = 'in/pipeline.yml'


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _build_pagination_cfg(
    pagination: CfgPaginationConfig | None,
    pag_overrides: Mapping[str, Any] | None,
) -> ApiPaginationConfig | None:
    """
    Build a pagination config mapping for the client from an optional
    PaginationConfig-like object and optional overrides.

    Parameters
    ----------
    pagination: PaginationConfig | None
        The pagination configuration to use.
    pag_overrides: Mapping[str, Any] | None
        Overrides to apply to the pagination configuration.

    Returns
    -------
    ApiPaginationConfig | None
        The built API pagination configuration or None if not determinable.

    Notes
    -----
    - Returns None when no pagination type can be determined.
    - Inputs can be dataclass-like objects (with attributes) or None.
    - Job-level overrides (``pag_overrides``) take precedence over the
      values on ``pagination``.
    """

    ptype = None
    records_path = None
    max_pages = None
    max_records = None
    if pagination:
        ptype = (getattr(pagination, 'type', '') or '').strip().lower()
        records_path = getattr(pagination, 'records_path', None)
        max_pages = getattr(pagination, 'max_pages', None)
        max_records = getattr(pagination, 'max_records', None)
    if pag_overrides:
        ptype = (pag_overrides.get('type') or ptype or '').strip().lower()
        records_path = pag_overrides.get('records_path', records_path)
        max_pages = pag_overrides.get('max_pages', max_pages)
        max_records = pag_overrides.get('max_records', max_records)

    if not ptype:
        return None

    pag_cfg: dict[str, Any] = {
        'type': ptype,
        'records_path': records_path,
        'max_pages': max_pages,
        'max_records': max_records,
    }

    if ptype in {'page', 'offset'}:
        page_param = pag_overrides.get('page_param') if pag_overrides else None
        size_param = pag_overrides.get('size_param') if pag_overrides else None
        start_page = pag_overrides.get('start_page') if pag_overrides else None
        page_size = pag_overrides.get('page_size') if pag_overrides else None
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
        pag_cfg.update(
            {
                'page_param': str(page_param or 'page'),
                'size_param': str(size_param or 'per_page'),
                'start_page': int(start_page or 1),
                'page_size': int(page_size or 100),
            },
        )
    elif ptype == 'cursor':
        cursor_param = (
            pag_overrides.get('cursor_param') if pag_overrides else None
        )
        cursor_path = (
            pag_overrides.get('cursor_path') if pag_overrides else None
        )
        page_size = pag_overrides.get('page_size') if pag_overrides else None
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
        pag_cfg.update(
            {
                'cursor_param': str(cursor_param or 'cursor'),
                'cursor_path': cursor_path,
                'page_size': int(page_size or 100),
                'start_cursor': start_cursor,
            },
        )

    return cast(ApiPaginationConfig, pag_cfg)


def _build_session_from_config(
    cfg: dict[str, Any] | None,
) -> requests.Session:
    """
    Create a requests.Session from a simple config mapping.

    Supported keys: headers, params, auth, verify, cert, proxies,
    cookies, trust_env.

    Parameters
    ----------
    cfg : dict[str, Any] | None
        Configuration mapping.

    Returns
    -------
    requests.Session
        Configured requests.Session instance.

    Notes
    -----
    - Only known keys are respected and safely ignored if malformed.
    - ``Session.params`` may not exist on very old ``requests`` versions; the
      setattr is guarded accordingly.
    """

    s = requests.Session()
    if not cfg:
        return s
    headers = cfg.get('headers')
    if isinstance(headers, dict):
        s.headers.update(headers)
    params = cfg.get('params')
    if isinstance(params, dict):
        # requests supports Session.params on recent versions
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


def _compute_rl_sleep_seconds(
    rate_limit: CfgRateLimitConfig | Mapping[str, Any] | None,
    overrides: Mapping[str, Any] | None,
) -> float | None:
    """
    Compute sleep_seconds from a rate_limit dataclass/mapping plus overrides.

    Parameters
    ----------
    rate_limit : Any | Mapping[str, Any] | None
        The rate limit configuration to use.
    overrides : Mapping[str, Any] | None
        Overrides to apply to the rate limit configuration.

    Returns
    -------
    float | None
        The computed sleep seconds or None if not determinable.

    Notes
    -----
    - Accepts either a dataclass-like object (with ``sleep_seconds`` and
      ``max_per_sec`` attributes) or a Mapping in the same shape.
    - Calls module-level ``compute_sleep_seconds`` so tests can monkeypatch it
      via ``etlplus.run.compute_sleep_seconds``.
    """

    rl_map: Mapping[str, Any] | None
    if rate_limit and hasattr(rate_limit, 'sleep_seconds'):
        rl_map = {
            'sleep_seconds': getattr(rate_limit, 'sleep_seconds', None),
            'max_per_sec': getattr(rate_limit, 'max_per_sec', None),
        }
    else:
        rl_map = cast(Mapping[str, Any] | None, rate_limit)
    return compute_sleep_seconds(cast(Any, rl_map), overrides or {})


def _merge_session_cfg_three(
    api_cfg: Any,
    ep: Any,
    source_session_cfg: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """
    Merge session config dictionaries from api -> endpoint -> source.

    Parameters
    ----------
    api_cfg : Any
        API config object (may have a 'session' dict attribute).
    ep : Any
        Endpoint config object (may have a 'session' dict attribute).
    source_session_cfg : dict[str, Any] | None
        Source-level session config overrides.

    Returns
    -------
    dict[str, Any] | None
        Merged session configuration or None if empty.

    Notes
    -----
    - Precedence: API < Endpoint < Source (later updates override earlier).
    - Returns None instead of an empty dict for downstream convenience.
    """

    api_sess = getattr(api_cfg, 'session', None)
    ep_sess = getattr(ep, 'session', None)
    merged: dict[str, Any] = {}
    if isinstance(api_sess, dict):
        merged.update(api_sess)
    if isinstance(ep_sess, dict):
        merged.update(ep_sess)
    if isinstance(source_session_cfg, dict):
        merged.update(source_session_cfg)
    return merged or None


def _paginate_with_client(
    client: Any,
    endpoint_key: str,
    params: Mapping[str, Any] | None,
    headers: Mapping[str, str] | None,
    timeout: Any,
    pagination: Any,
    sleep_seconds: float | None,
) -> Any:
    """
    Call client.paginate with kwargs matching its signature (supports
    FakeClient underscores).

    Parameters
    ----------
    client : Any
        The client instance to use for pagination.
    endpoint_key : str
        The endpoint key to paginate.
    params : Mapping[str, Any] | None
        The parameters to include in the request.
    headers : Mapping[str, str] | None
        The headers to include in the request.
    timeout : Any
        The timeout configuration to use.
    pagination : Any
        The pagination configuration to use.
    sleep_seconds : float | None
        The sleep seconds configuration to use.

    Returns
    -------
    Any
        The result of the pagination call.

    Notes
    -----
    - Some tests use a FakeClient whose parameter names begin with underscores
      (e.g., ``_params``); this helper inspects the callable to populate the
      correct keyword names.
    - ``sleep_seconds`` is normalized to ``float`` (None -> 0.0) to match the
      client typing.
    """

    sig = inspect.signature(client.paginate)  # type: ignore[arg-type]
    kw_pag: dict[str, Any] = {
        'pagination': pagination,
    }
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


# SECTION: FUNCTIONS ======================================================== #


def run(
    job: str,
    config_path: str | None = None,
) -> JSONDict:
    """
    Run a pipeline job defined in a YAML configuration.

    This mirrors the run-mode logic from ``etlplus.cli.cmd_pipeline``
    (without the list/summary modes). By default it reads the configuration
    from ``in/pipeline.yml``, but callers can provide an explicit
    ``config_path`` to override this.

    Parameters
    ----------
    job : str
        Job name to execute.

    Returns
    -------
    JSONDict
        Result dictionary.
    """

    cfg_path = config_path or DEFAULT_CONFIG_PATH
    cfg = load_pipeline_config(cfg_path, substitute=True)

    # Lookup job by name
    job_obj = next((j for j in cfg.jobs if j.name == job), None)
    if not job_obj:
        raise ValueError(f'Job not found: {job}')

    # Index sources/targets by name
    sources_by_name = {getattr(s, 'name', None): s for s in cfg.sources}
    targets_by_name = {getattr(t, 'name', None): t for t in cfg.targets}

    # Extract.
    if not job_obj.extract:
        raise ValueError('Job missing "extract" section')
    source_name = job_obj.extract.source
    if source_name not in sources_by_name:
        raise ValueError(f'Unknown source: {source_name}')
    source_obj = sources_by_name[source_name]
    ex_opts: dict[str, Any] = job_obj.extract.options or {}

    data: Any
    stype = getattr(source_obj, 'type', None)
    match stype:
        case 'file':
            path = getattr(source_obj, 'path', None)
            fmt = ex_opts.get('format') or getattr(
                source_obj, 'format', 'json',
            )
            if not path:
                raise ValueError('File source missing "path"')
            data = extract('file', path, format=fmt)
        case 'database':
            conn = getattr(source_obj, 'connection_string', '')
            data = extract('database', conn)
        case 'api':
            # Build URL, params, headers, pagination, rate_limit, retry,
            # session config.
            url: str | None = getattr(source_obj, 'url', None)
            params: dict[str, Any] = dict(
                getattr(source_obj, 'query_params', {}) or {},
            )
            headers: dict[str, str] = dict(
                getattr(source_obj, 'headers', {}) or {},
            )
            pagination = getattr(source_obj, 'pagination', None)
            rate_limit = getattr(source_obj, 'rate_limit', None)
            retry = getattr(source_obj, 'retry', None)
            retry_network_errors = bool(
                getattr(source_obj, 'retry_network_errors', False),
            )
            session_cfg = getattr(source_obj, 'session', None)

            api_name = getattr(source_obj, 'api', None)
            endpoint_name = getattr(source_obj, 'endpoint', None)

            # When an API + endpoint are referenced, compose using ApiConfig
            # and prefer ApiConfig helpers for correctness (e.g., base_path).
            use_client_endpoints = False
            client_base_url: str | None = None
            client_base_path: str | None = None
            client_endpoints_map: dict[str, str] | None = None
            selected_endpoint_key: str | None = None
            if api_name and endpoint_name:
                api_cfg = cfg.apis.get(api_name)
                if not api_cfg:
                    raise ValueError(f'API not defined: {api_name}')
                ep = api_cfg.endpoints.get(endpoint_name)
                if not ep:
                    raise ValueError(
                        f'Endpoint "{endpoint_name}" not defined in API '
                        f'"{api_name}"',
                    )

                # Compose and inherit, using helpers for accuracy.
                url = api_cfg.build_endpoint_url(ep)
                params = {**ep.query_params, **params}
                headers = {**api_cfg.headers, **headers}

                # Inherit pagination/rate_limit in order:
                # source -> endpoint -> API profile defaults
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
                retry = retry or getattr(ep, 'retry', None) or getattr(
                    api_cfg, 'retry', None,
                )
                retry_network_errors = (
                    retry_network_errors
                    or bool(getattr(ep, 'retry_network_errors', False))
                    or bool(
                        getattr(api_cfg, 'retry_network_errors', False),
                    )
                )
                # Merge session config: api -> endpoint -> source
                session_cfg = _merge_session_cfg_three(
                    api_cfg,
                    ep,
                    session_cfg,
                )

                # Prepare EndpointClient instantiation using base_url +
                # base_path (passed via the client's base_path parameter)
                # so relative endpoint paths are resolved consistently.
                use_client_endpoints = True
                client_base_url = api_cfg.base_url
                client_base_path = api_cfg.effective_base_path()
                client_endpoints_map = {
                    k: v.path for k, v in api_cfg.endpoints.items()
                }
                selected_endpoint_key = endpoint_name

            # Apply overrides from job.extract.options.
            params |= ex_opts.get('query_params', {})
            headers |= ex_opts.get('headers', {})
            timeout = ex_opts.get('timeout')
            pag_ov = ex_opts.get('pagination', {})
            rl_ov = ex_opts.get('rate_limit', {})
            rty_ov = (
                ex_opts.get('retry') if 'retry' in ex_opts else None
            )
            rne_ov = (
                ex_opts.get('retry_network_errors')
                if 'retry_network_errors' in ex_opts
                else None
            )
            sess_ov = ex_opts.get('session', {})

            # Compute rate limit sleep using consolidated helper.
            sleep_s = _compute_rl_sleep_seconds(rate_limit, rl_ov)

            # Apply retry overrides (if present).
            if rty_ov is not None:
                retry = rty_ov
            if rne_ov is not None:
                retry_network_errors = bool(rne_ov)

            # Apply session overrides (merge).
            if isinstance(sess_ov, dict):
                base_cfg = dict(session_cfg or {})
                base_cfg.update(sess_ov)
                session_cfg = base_cfg

            # Build pagination config via helper
            pag_cfg: ApiPaginationConfig | None = _build_pagination_cfg(
                pagination,
                pag_ov,
            )

            # Build session object if config provided.
            sess_obj = (
                _build_session_from_config(session_cfg)
                if isinstance(session_cfg, dict)
                else None
            )

            if (
                use_client_endpoints
                and client_base_url
                and client_endpoints_map
                and selected_endpoint_key
            ):
                # Instantiate client with effective_base_url and known
                # endpoints.
                client = EndpointClient(
                    base_url=client_base_url,
                    base_path=client_base_path,
                    endpoints=client_endpoints_map,
                    retry=retry,
                    retry_network_errors=retry_network_errors,
                    session=sess_obj,
                )
                # Call paginate via consolidated helper
                data = _paginate_with_client(
                    client,
                    selected_endpoint_key,
                    params,
                    headers,
                    timeout,
                    cast(ApiPaginationConfig | None, pag_cfg),
                    sleep_s,
                )
            else:
                if not url:
                    raise ValueError('API source missing URL')

                # Use instance-based pagination via EndpointClient for
                # absolute URL.
                parts = urlsplit(url)
                base = urlunsplit((parts.scheme, parts.netloc, '', '', ''))
                client = EndpointClient(
                    base_url=base,
                    endpoints={},
                    retry=retry,
                    retry_network_errors=retry_network_errors,
                    session=sess_obj,
                )
                data = client.paginate_url(
                    url,
                    params,
                    headers,
                    timeout,
                    cast(ApiPaginationConfig | None, pag_cfg),
                    sleep_seconds=(sleep_s or 0.0),
                )
        case _:
            raise ValueError(f'Unsupported source type: {stype}')

    # DRY: unified validation helper (pre/post transform)
    val_ref = job_obj.validate
    enabled_validation = val_ref is not None
    if enabled_validation:
        # Type narrowing for static checkers
        assert val_ref is not None
        rules = cfg.validations.get(val_ref.ruleset, {})
        severity = (
            (val_ref.severity or 'error').lower()
        )
        phase = (
            (val_ref.phase or 'before_transform').lower()
        )
    else:
        rules = {}
        severity = 'error'
        phase = 'before_transform'

    # Pre-transform validation (if configured).
    data = maybe_validate(
        data,
        'before_transform',
        enabled=enabled_validation,
        rules=rules,
        phase=phase,
        severity=severity,
        validate_fn=validate,  # type: ignore[arg-type]
        print_json_fn=print_json,
    )

    # Transform (optional).
    if job_obj.transform:
        ops: Any = cfg.transforms.get(job_obj.transform.pipeline, {})
        data = transform(data, ops)

    # Post-transform validation (if configured)
    data = maybe_validate(
        data,
        'after_transform',
        enabled=enabled_validation,
        rules=rules,
        phase=phase,
        severity=severity,
        validate_fn=validate,  # type: ignore[arg-type]
        print_json_fn=print_json,
    )

    # Load.
    if not job_obj.load:
        raise ValueError('Job missing "load" section')
    target_name = job_obj.load.target
    if target_name not in targets_by_name:
        raise ValueError(f'Unknown target: {target_name}')
    target_obj = targets_by_name[target_name]
    overrides = job_obj.load.overrides or {}

    ttype = getattr(target_obj, 'type', None)
    match ttype:
        case 'file':
            path = (
                overrides.get('path')
                or getattr(target_obj, 'path', None)
            )
            fmt = overrides.get('format') or getattr(
                target_obj, 'format', 'json',
            )
            if not path:
                raise ValueError('File target missing "path"')
            result = load(data, 'file', path, format=fmt)
        case 'api':
            url = overrides.get('url') or getattr(target_obj, 'url', None)
            method = overrides.get('method') or getattr(
                target_obj, 'method', 'post',
            )
            headers = {
                **(getattr(target_obj, 'headers', {}) or {}),
                **overrides.get('headers', {}),
            }
            kwargs: dict[str, Any] = {}
            if headers:
                kwargs['headers'] = headers
            if 'timeout' in overrides:
                kwargs['timeout'] = overrides['timeout']
            # Support service + endpoint composition similar to Sources.
            tgt_api_name = getattr(target_obj, 'api', None)
            tgt_endpoint_name = getattr(target_obj, 'endpoint', None)
            if tgt_api_name and tgt_endpoint_name and not url:
                api_cfg = cfg.apis.get(tgt_api_name)
                if not api_cfg:
                    raise ValueError(f'API not defined: {tgt_api_name}')
                ep = api_cfg.endpoints.get(tgt_endpoint_name)
                if not ep:
                    raise ValueError(
                        f'Endpoint "{tgt_endpoint_name}" not defined in '
                        f'API "{tgt_api_name}"',
                    )
                url = api_cfg.build_endpoint_url(ep)
                # Merge API-level headers with target headers
                kwargs['headers'] = {
                    **api_cfg.headers,
                    **(kwargs.get('headers') or {}),
                }
            if not url:
                raise ValueError('API target missing "url"')
            result = load(data, 'api', url, method=method, **kwargs)
        case 'database':
            conn = overrides.get('connection_string') or getattr(
                target_obj, 'connection_string', '',
            )
            result = load(data, 'database', str(conn))
        case _:
            raise ValueError(f'Unsupported target type: {ttype}')

    # Return the terminal load result directly; callers (e.g., CLI) can wrap
    # it in their own envelope when needed.
    return cast(JSONDict, result)
