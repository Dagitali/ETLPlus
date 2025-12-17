"""
:mod:`etlplus.run` module.

A module for running ETL jobs defined in YAML configurations.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import Final
from typing import TypedDict
from typing import cast
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

import requests  # type: ignore[import]

from .api import EndpointClient  # noqa: F401 (re-exported for tests)
from .api import PaginationConfigMap
from .api import RequestOptions
from .api import RetryPolicy
from .api import Url
from .config import load_pipeline_config
from .enums import DataConnectorType
from .enums import coerce_data_connector_type
from .extract import extract
from .load import load
from .run_helpers import compose_api_request_env
from .run_helpers import compose_api_target_env
from .run_helpers import paginate_with_client
from .transform import transform
from .types import JSONDict
from .types import Timeout
from .utils import print_json
from .validate import validate
from .validation.utils import maybe_validate

# SECTION: EXPORTS ========================================================== #


__all__ = ['run']


# SECTION: TYPED DICTS ====================================================== #


class BaseApiHttpEnv(TypedDict, total=False):
    """
    Common HTTP request environment for API interactions.

    Fields shared by both source-side and target-side API operations.
    """

    # Request details
    url: Url | None
    headers: dict[str, str]
    timeout: Timeout

    # Session
    session: requests.Session | None


class ApiRequestEnv(BaseApiHttpEnv, total=False):
    """
    Composed request environment for API sources.

    Returned by ``compose_api_request_env`` (run_helpers) and consumed by the
    API extract branch. Values are fully merged with endpoint/API defaults and
    job-level overrides, preserving the original precedence and behavior.
    """

    # Client
    use_endpoints: bool
    base_url: str | None
    base_path: str | None
    endpoints_map: dict[str, str] | None
    endpoint_key: str | None

    # Request
    params: dict[str, Any]
    pagination: PaginationConfigMap | None
    sleep_seconds: float

    # Reliability
    retry: RetryPolicy | None
    retry_network_errors: bool


class ApiTargetEnv(BaseApiHttpEnv, total=False):
    """
    Composed request environment for API targets.

    Returned by ``compose_api_target_env`` (run_helpers) and consumed by the
    API load branch. Values are merged from the target object, optional
    API/endpoint reference, and job-level overrides, preserving original
    precedence and behavior.

    Notes
    -----
    - Precedence for inherited values matches original logic:
        overrides -> target -> API profile defaults.
    - Target composition does not include pagination/rate-limit/retry since
        loads are single-request operations; only headers/timeout/session
        apply.
    """

    # Request
    method: str | None


class SessionConfig(TypedDict, total=False):
    """
    Minimal session configuration schema accepted by this runner.

    Keys mirror common requests.Session options; all are optional.
    """

    headers: Mapping[str, Any]
    params: Mapping[str, Any]
    auth: Any  # (user, pass) tuple or requests-compatible auth object
    verify: bool | str
    cert: Any  # str or (cert, key)
    proxies: Mapping[str, Any]
    cookies: Mapping[str, Any]
    trust_env: bool


# SECTION: CONSTANTS ======================================================== #


DEFAULT_CONFIG_PATH: Final[str] = 'in/pipeline.yml'


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
    config_path : str | None, optional
        Path to the pipeline YAML configuration. Defaults to
        ``in/pipeline.yml``.

    Returns
    -------
    JSONDict
        Result dictionary.

    Raises
    ------
    ValueError
        If the job is not found or if there are configuration issues.
    """
    cfg_path = config_path or DEFAULT_CONFIG_PATH
    cfg = load_pipeline_config(cfg_path, substitute=True)

    # Lookup job by name
    if not (job_obj := next((j for j in cfg.jobs if j.name == job), None)):
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
    stype_raw = getattr(source_obj, 'type', None)
    stype = coerce_data_connector_type(stype_raw or '')
    match stype:
        case DataConnectorType.FILE:
            path = getattr(source_obj, 'path', None)
            fmt = ex_opts.get('format') or getattr(
                source_obj,
                'format',
                'json',
            )
            if not path:
                raise ValueError('File source missing "path"')
            data = extract('file', path, file_format=fmt)
        case DataConnectorType.DATABASE:
            conn = getattr(source_obj, 'connection_string', '')
            data = extract('database', conn)
        case DataConnectorType.API:
            env = compose_api_request_env(cfg, source_obj, ex_opts)
            if (
                env.get('use_endpoints')
                and env.get('base_url')
                and env.get('endpoints_map')
                and env.get('endpoint_key')
            ):
                # Construct client using module-level EndpointClient so tests
                # can monkeypatch this class on etlplus.run.
                ClientClass = EndpointClient  # noqa: N806
                client = ClientClass(
                    base_url=cast(str, env['base_url']),
                    base_path=cast(str | None, env.get('base_path')),
                    endpoints=cast(dict[str, str], env['endpoints_map']),
                    retry=env.get('retry'),
                    retry_network_errors=bool(
                        env.get('retry_network_errors', False),
                    ),
                    session=env.get('session'),
                )
                data = paginate_with_client(
                    client,
                    cast(str, env['endpoint_key']),
                    env.get('params'),
                    env.get('headers'),
                    env.get('timeout'),
                    env.get('pagination'),
                    cast(float | None, env.get('sleep_seconds')),
                )
            else:
                url = env.get('url')
                if not url:
                    raise ValueError('API source missing URL')
                parts = urlsplit(cast(str, url))
                base = urlunsplit((parts.scheme, parts.netloc, '', '', ''))
                ClientClass = EndpointClient  # noqa: N806
                client = ClientClass(
                    base_url=base,
                    base_path=None,
                    endpoints={},
                    retry=env.get('retry'),
                    retry_network_errors=bool(
                        env.get('retry_network_errors', False),
                    ),
                    session=env.get('session'),
                )

                request_options = RequestOptions(
                    params=cast(Mapping[str, Any] | None, env.get('params')),
                    headers=cast(Mapping[str, str] | None, env.get('headers')),
                    timeout=cast(Timeout | None, env.get('timeout')),
                )

                data = client.paginate_url(
                    cast(str, url),
                    cast(PaginationConfigMap | None, env.get('pagination')),
                    request=request_options,
                    sleep_seconds=cast(float, env.get('sleep_seconds', 0.0)),
                )
        case _:
            # ``coerce_data_connector_type`` already raises for invalid
            # connector types; this branch is defensive only.
            raise ValueError(f'Unsupported source type: {stype_raw}')

    # DRY: unified validation helper (pre/post transform)
    val_ref = job_obj.validate
    enabled_validation = val_ref is not None
    if enabled_validation:
        # Type narrowing for static checkers
        assert val_ref is not None
        rules = cfg.validations.get(val_ref.ruleset, {})
        severity = (val_ref.severity or 'error').lower()
        phase = (val_ref.phase or 'before_transform').lower()
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

    ttype_raw = getattr(target_obj, 'type', None)
    ttype = coerce_data_connector_type(ttype_raw or '')
    match ttype:
        case DataConnectorType.FILE:
            path = overrides.get('path') or getattr(target_obj, 'path', None)
            fmt = overrides.get('format') or getattr(
                target_obj,
                'format',
                'json',
            )
            if not path:
                raise ValueError('File target missing "path"')
            result = load(data, 'file', path, file_format=fmt)
        case DataConnectorType.API:
            env_t = compose_api_target_env(cfg, target_obj, overrides)
            url_t = env_t.get('url')
            if not url_t:
                raise ValueError('API target missing "url"')
            kwargs_t: dict[str, Any] = {}
            if env_t.get('headers'):
                kwargs_t['headers'] = cast(dict[str, str], env_t['headers'])
            if env_t.get('timeout') is not None:
                kwargs_t['timeout'] = env_t['timeout']
            if env_t.get('session') is not None:
                kwargs_t['session'] = env_t['session']
            result = load(
                data,
                'api',
                cast(str, url_t),
                method=cast(str | Any, env_t.get('method') or 'post'),
                **kwargs_t,
            )
        case DataConnectorType.DATABASE:
            conn = overrides.get('connection_string') or getattr(
                target_obj,
                'connection_string',
                '',
            )
            result = load(data, 'database', str(conn))
        case _:
            # ``coerce_data_connector_type`` already raises for invalid
            # connector types; this branch is defensive only.
            raise ValueError(f'Unsupported target type: {ttype_raw}')

    # Return the terminal load result directly; callers (e.g., CLI) can wrap
    # it in their own envelope when needed.
    return cast(JSONDict, result)
