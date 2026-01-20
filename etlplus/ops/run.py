"""
:mod:`etlplus.ops.run` module.

A module for running ETL jobs defined in YAML configurations.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import Final
from typing import cast
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from ..api import EndpointClient  # noqa: F401 (re-exported for tests)
from ..api import PaginationConfigMap
from ..api import RequestOptions
from ..api import compose_api_request_env
from ..api import compose_api_target_env
from ..api import paginate_with_client
from ..config import load_pipeline_config
from ..enums import DataConnectorType
from ..types import JSONDict
from ..types import Timeout
from ..utils import print_json
from .extract import extract
from .load import load
from .transform import transform
from .utils import maybe_validate
from .validate import validate

# SECTION: EXPORTS ========================================================== #


__all__ = ['run']


# SECTION: CONSTANTS ======================================================== #


DEFAULT_CONFIG_PATH: Final[str] = 'in/pipeline.yml'


# SECTION: FUNCTIONS ======================================================== #


def run(
    job: str,
    config_path: str | None = None,
) -> JSONDict:
    """
    Run a pipeline job defined in a YAML configuration.

    By default it reads the configuration from ``in/pipeline.yml``, but callers
    can provide an explicit ``config_path`` to override this.

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
    match DataConnectorType.coerce(stype_raw or ''):
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
                # can monkeypatch this class on etlplus.ops.run.
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
            # :meth:`coerce` already raises for invalid connector types, but
            # keep explicit guard for defensive programming.
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
    match DataConnectorType.coerce(ttype_raw or ''):
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
            # :meth:`coerce` already raises for invalid connector types, but
            # keep explicit guard for defensive programming.
            raise ValueError(f'Unsupported target type: {ttype_raw}')

    # Return the terminal load result directly; callers (e.g., CLI) can wrap
    # it in their own envelope when needed.
    return cast(JSONDict, result)
