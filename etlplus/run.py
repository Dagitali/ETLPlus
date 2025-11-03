"""
etlplus.load
============

A module for loading data into target files, databases, and REST APIs.
"""
from __future__ import annotations

from typing import Any
from typing import cast
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

import requests  # type: ignore

from .api import compute_sleep_seconds
from .api import EndpointClient
from .api import PaginationConfig
from .config import load_pipeline_config
from .extract import extract
from .load import load
from .transform import transform
from .types import JSONDict
from .utils import print_json
from .validate import validate
from .validation.utils import maybe_validate


# SECTION: PUBLIC API ======================================================= #


__all__ = ['run']


# SECTION: CONSTANTS ======================================================== #


DEFAULT_CONFIG_PATH = 'in/pipeline.yml'


# SECTION: PROTECTED FUNCTIONS ============================================== #


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
        except Exception:
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
        except Exception:
            pass
    if 'trust_env' in cfg:
        try:
            # type: ignore[attr-defined]
            s.trust_env = bool(cfg.get('trust_env'))
        except Exception:
            pass

    return s


# SECTION: FUNCTIONS ======================================================== #


def run(
    job: str,
) -> JSONDict:
    """
    Run a pipeline job defined in the default YAML configuration.

    This mirrors the run-mode logic from ``etlplus.cli.cmd_pipeline``
    (without the list/summary modes) and accepts only the job name.

    Parameters
    ----------
    job : str
        Job name to execute.

    Returns
    -------
    JSONDict
        Result dictionary.
    """

    cfg = load_pipeline_config(DEFAULT_CONFIG_PATH, substitute=True)

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
                getattr(source_obj, 'params', {}) or {},
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
                # Compose and inherit
                base = api_cfg.base_url.rstrip('/')
                path = ep.path.lstrip('/')
                url = f'{base}/{path}'
                params = {**ep.params, **params}
                headers = {**api_cfg.headers, **headers}
                pagination = pagination or ep.pagination
                rate_limit = rate_limit or ep.rate_limit
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
                api_sess = getattr(api_cfg, 'session', None)
                ep_sess = getattr(ep, 'session', None)
                merged: dict[str, Any] = {}
                if isinstance(api_sess, dict):
                    merged.update(api_sess)
                if isinstance(ep_sess, dict):
                    merged.update(ep_sess)
                if isinstance(session_cfg, dict):
                    merged.update(session_cfg)
                session_cfg = merged or None

            # Apply overrides from job.extract.options.
            params |= ex_opts.get('params', {})
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

            # Compute rate limit sleep using helper
            sleep_s = compute_sleep_seconds(rate_limit, rl_ov)

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

            # Pagination params
            ptype = None
            records_path = None
            max_pages = None
            max_records = None
            if pagination:
                ptype = (pagination.type or '').strip().lower()
                records_path = pagination.records_path
                max_pages = pagination.max_pages
                max_records = pagination.max_records
            # Override with job-level
            if pag_ov:
                ptype = (pag_ov.get('type') or ptype or '').strip().lower()
                records_path = pag_ov.get('records_path', records_path)
                max_pages = pag_ov.get('max_pages', max_pages)
                max_records = pag_ov.get('max_records', max_records)

            # Delegate to pagination helper.
            pag_cfg: dict[str, Any] | None = None
            if ptype:
                pag_cfg = {
                    'type': ptype,
                    'records_path': records_path,
                    'max_pages': max_pages,
                    'max_records': max_records,
                }
                if ptype in {'page', 'offset'}:
                    page_param = (
                        pag_ov.get('page_param') if pag_ov else None
                    )
                    size_param = (
                        pag_ov.get('size_param') if pag_ov else None
                    )
                    start_page = (
                        pag_ov.get('start_page') if pag_ov else None
                    )
                    page_size = (
                        pag_ov.get('page_size') if pag_ov else None
                    )
                    if pagination:
                        page_param = (
                            page_param or pagination.page_param or 'page'
                        )
                        size_param = (
                            size_param
                            or pagination.size_param
                            or 'per_page'
                        )
                        start_page = (
                            start_page or pagination.start_page or 1
                        )
                        page_size = (
                            page_size or pagination.page_size or 100
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
                        pag_ov.get('cursor_param') if pag_ov else None
                    )
                    cursor_path = (
                        pag_ov.get('cursor_path') if pag_ov else None
                    )
                    page_size = pag_ov.get('page_size') if pag_ov else None
                    start_cursor = None
                    if pagination:
                        cursor_param = (
                            cursor_param
                            or pagination.cursor_param
                            or 'cursor'
                        )
                        cursor_path = (
                            cursor_path or pagination.cursor_path
                        )
                        page_size = (
                            page_size or pagination.page_size or 100
                        )
                        start_cursor = pagination.start_cursor
                    pag_cfg.update(
                        {
                            'cursor_param': str(cursor_param or 'cursor'),
                            'cursor_path': cursor_path,
                            'page_size': int(page_size or 100),
                            'start_cursor': start_cursor,
                        },
                    )

            if not url:
                raise ValueError('API source missing URL')
            # Use instance-based pagination via EndpointClient.
            parts = urlsplit(url)
            base = urlunsplit((parts.scheme, parts.netloc, '', '', ''))

            # Build session object if config provided.
            sess_obj = (
                _build_session_from_config(session_cfg)
                if isinstance(session_cfg, dict)
                else None
            )
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
                cast(PaginationConfig | None, pag_cfg),
                sleep_seconds=sleep_s,
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

    # Pre-transform validation (if configured)
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

    return {'status': 'ok', 'result': result}
