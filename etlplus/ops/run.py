"""
:mod:`etlplus.ops.run` module.

A module for running ETL jobs defined in YAML configurations.
"""

from __future__ import annotations

from typing import Any
from typing import Final
from typing import cast

from ..api import HttpMethod
from ..connector import DataConnectorType
from ..file import FileFormat
from ..types import JSONData
from ..types import JSONDict
from ..types import PipelineConfig
from ..types import StrPath
from ..utils import print_json
from ..workflow import load_pipeline_config
from .extract import extract
from .extract import extract_from_api_source
from .load import load
from .load import load_to_api_target
from .transform import transform
from .utils import maybe_validate
from .validate import validate

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'run',
    'run_pipeline',
]


# SECTION: CONSTANTS ======================================================== #


DEFAULT_CONFIG_PATH: Final[str] = 'in/pipeline.yml'


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _index_connectors(
    connectors: list[Any],
    *,
    label: str,
) -> dict[str, Any]:
    """
    Index connectors by name with a helpful error on duplicates.

    Parameters
    ----------
    connectors : list[Any]
        Connector objects to index.
    label : str
        Label used in error messages (e.g., ``"source"``).

    Returns
    -------
    dict[str, Any]
        Mapping of connector names to connector objects.

    Raises
    ------
    ValueError
        If duplicate connector names are found.
    """
    indexed: dict[str, Any] = {}
    for connector in connectors:
        name = getattr(connector, 'name', None)
        if not isinstance(name, str) or not name:
            continue
        if name in indexed:
            raise ValueError(f'Duplicate {label} connector name: {name}')
        indexed[name] = connector
    return indexed


def _require_named_connector(
    connectors: dict[str, Any],
    name: str,
    *,
    label: str,
) -> Any:
    """
    Return a connector by name or raise a helpful error.

    Parameters
    ----------
    connectors : dict[str, Any]
        Mapping of connector names to connector objects.
    name : str
        Connector name to retrieve.
    label : str
        Label used in error messages (e.g., ``"source"``).

    Returns
    -------
    Any
        Connector object.

    Raises
    ------
    ValueError
        If the connector name is not found.
    """
    if name not in connectors:
        raise ValueError(f'Unknown {label}: {name}')
    return connectors[name]


def _resolve_validation_config(
    job_obj: Any,
    cfg: Any,
) -> tuple[bool, dict[str, Any], str, str]:
    """
    Resolve validation settings for a job with safe defaults.

    Parameters
    ----------
    job_obj : Any
        Job configuration object.
    cfg : Any
        Pipeline configuration object with validations.

    Returns
    -------
    tuple[bool, dict[str, Any], str, str]
        Tuple of (enabled, rules, severity, phase).
    """
    val_ref = job_obj.validate
    if val_ref is None:
        return False, {}, 'error', 'before_transform'

    rules = cfg.validations.get(val_ref.ruleset, {})
    severity = (val_ref.severity or 'error').lower()
    phase = (val_ref.phase or 'before_transform').lower()
    return True, rules, severity, phase


# SECTION: FUNCTIONS ======================================================== #


def run(
    job: str,
    config_path: str | None = None,
) -> JSONDict:
    """
    Run a pipeline job defined in a YAML configuration.

    By default it reads the configuration from ``in/pipeline.yml``, but callers
    can provide an explicit *config_path* to override this.

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
    sources_by_name = _index_connectors(cfg.sources, label='source')
    targets_by_name = _index_connectors(cfg.targets, label='target')

    # Extract.
    if not job_obj.extract:
        raise ValueError('Job missing "extract" section')
    source_name = job_obj.extract.source
    source_obj = _require_named_connector(
        sources_by_name,
        source_name,
        label='source',
    )
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
            data = extract_from_api_source(cfg, source_obj, ex_opts)
        case _:
            # :meth:`coerce` already raises for invalid connector types, but
            # keep explicit guard for defensive programming.
            raise ValueError(f'Unsupported source type: {stype_raw}')

    enabled_validation, rules, severity, phase = _resolve_validation_config(
        job_obj,
        cfg,
    )

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
    target_obj = _require_named_connector(
        targets_by_name,
        target_name,
        label='target',
    )
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
            result = load_to_api_target(cfg, target_obj, overrides, data)
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


def run_pipeline(
    *,
    source_type: DataConnectorType | str | None = None,
    source: StrPath | JSONData | None = None,
    operations: PipelineConfig | None = None,
    target_type: DataConnectorType | str | None = None,
    target: StrPath | None = None,
    file_format: FileFormat | str | None = None,
    method: HttpMethod | str | None = None,
    **kwargs: Any,
) -> JSONData:
    """
    Run a single extract-transform-load flow without a YAML config.

    Parameters
    ----------
    source_type : DataConnectorType | str | None, optional
        Connector type for extraction. When ``None``, *source* is assumed
        to be pre-loaded data and extraction is skipped.
    source : StrPath | JSONData | None, optional
        Data source for extraction or the pre-loaded payload when
        *source_type* is ``None``.
    operations : PipelineConfig | None, optional
        Transform configuration passed to :func:`etlplus.ops.transform`.
    target_type : DataConnectorType | str | None, optional
        Connector type for loading. When ``None``, load is skipped and the
        transformed data is returned.
    target : StrPath | None, optional
        Target for loading (file path, connection string, or API URL).
    file_format : FileFormat | str | None, optional
        File format for file sources/targets (forwarded to extract/load).
    method : HttpMethod | str | None, optional
        HTTP method for API loads (forwarded to :func:`etlplus.ops.load`).
    **kwargs : Any
        Extra keyword arguments forwarded to extract/load for API options
        (headers, timeout, session, etc.).

    Returns
    -------
    JSONData
        Transformed data or the load result payload.

    Raises
    ------
    TypeError
        Raised when extracted data is not a dict or list of dicts and no
        target is specified.
    ValueError
        Raised when required source/target inputs are missing.
    """
    if source_type is None:
        if source is None:
            raise ValueError('source or source_type is required')
        data = source
    else:
        if source is None:
            raise ValueError('source is required when source_type is set')
        data = extract(
            source_type,
            cast(StrPath, source),
            file_format=file_format,
            **kwargs,
        )

    if operations:
        data = transform(data, operations)

    if target_type is None:
        if not isinstance(data, (dict, list)):
            raise TypeError(
                f'Expected data to be dict or list of dicts, '
                f'got {type(data).__name__}',
            )
        return data
    if target is None:
        raise ValueError('target is required when target_type is set')

    return load(
        data,
        target_type,
        target,
        file_format=file_format,
        method=method,
        **kwargs,
    )
