"""
:mod:`etlplus.ops.run` module.

A module for running ETL jobs defined in YAML configurations.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from time import perf_counter
from typing import Any
from typing import Final
from typing import Self
from typing import cast

from .._config import Config
from ..api import HttpMethod
from ..connector import DataConnectorType
from ..file._core import FileFormatArg
from ..utils import print_json
from ..utils._types import JSONData
from ..utils._types import JSONDict
from ..utils._types import StrPath
from ..workflow import topological_sort_jobs
from ._mappings import index_named_items
from ._mappings import merge_mapping_options
from ._types import PipelineConfig
from ._validation import ValidationResultDict
from ._validation import maybe_validate
from .extract import extract
from .extract import extract_from_api_source
from .load import load
from .load import load_to_api_target
from .transform import transform
from .validate import FieldRulesDict
from .validate import validate

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'run',
    'run_pipeline',
]


# SECTION: CONSTANTS ======================================================== #


DEFAULT_CONFIG_PATH: Final[str] = 'in/pipeline.yml'


@dataclass(frozen=True, slots=True)
class _DagJobRef:
    """
    Minimal job view used for DAG ordering across tolerant job-like inputs.
    """

    # -- Instance Attributes -- #

    name: str
    depends_on: list[str]


@dataclass(frozen=True, slots=True)
class _RunContext:
    """Resolved config and connector indexes used across one run."""

    # -- Instance Attributes -- #

    cfg: Any
    sources_by_name: dict[str, Any]
    targets_by_name: dict[str, Any]

    # -- Class Methods -- #

    @classmethod
    def from_config(
        cls,
        cfg: Any,
    ) -> Self:
        """Build a context with indexed sources and targets."""
        return cls(
            cfg=cfg,
            sources_by_name=_index_connectors(
                list(getattr(cfg, 'sources', []) or []),
                label='source',
            ),
            targets_by_name=_index_connectors(
                list(getattr(cfg, 'targets', []) or []),
                label='target',
            ),
        )


@dataclass(frozen=True, slots=True)
class _FileConnectorConfig:
    """Resolved file connector settings for one extract/load step."""

    # -- Instance Attributes -- #

    path: StrPath
    file_format: FileFormatArg
    options: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _JobValidationConfig:
    """Normalized per-job validation settings."""

    # -- Instance Attributes -- #

    enabled: bool
    rules: Mapping[str, Any]
    severity: str
    phase: str

    # -- Class Methods -- #

    @classmethod
    def from_job(
        cls,
        job_obj: Any,
        cfg: Any,
    ) -> Self:
        """Build validation settings for one configured job."""
        if (val_ref := getattr(job_obj, 'validate', None)) is None:
            return cls(
                enabled=False,
                rules={},
                severity='error',
                phase='before_transform',
            )

        validations = getattr(cfg, 'validations', {}) or {}
        rules = (
            validations.get(val_ref.ruleset, {})
            if isinstance(validations, Mapping)
            else {}
        )
        if not isinstance(rules, Mapping):
            rules = {}

        return cls(
            enabled=True,
            rules=dict(rules),
            severity=(val_ref.severity or 'error').lower(),
            phase=(val_ref.phase or 'before_transform').lower(),
        )

    # -- Instance Methods -- #

    def apply(
        self,
        data: JSONData,
        *,
        when: str,
    ) -> JSONData:
        """Validate one pipeline payload for the requested phase."""
        return maybe_validate(
            data,
            when,
            enabled=self.enabled,
            rules=self.rules,
            phase=self.phase,
            severity=self.severity,
            validate_fn=_validate_payload,
            print_json_fn=print_json,
        )


@dataclass(slots=True)
class _RunPlanTracker:
    """Accumulate execution state for one DAG-style run."""

    # -- Instance Attributes -- #

    ordered_job_names: list[str]
    requested_job: str | None
    continue_on_fail: bool
    mode: str
    failed_job_names: list[str] = field(default_factory=list)
    skipped_job_names: list[str] = field(default_factory=list)
    succeeded_job_names: list[str] = field(default_factory=list)
    executed_jobs: list[dict[str, Any]] = field(default_factory=list)
    final_job_name: str | None = None
    final_result: JSONDict | None = None
    final_result_status: str | None = None
    _failed_lookup: set[str] = field(default_factory=set, repr=False)
    _skipped_lookup: set[str] = field(default_factory=set, repr=False)

    # -- Instance Methods -- #

    def blocked_dependencies(
        self,
        job_obj: Any,
    ) -> list[str]:
        """Return failed/skipped upstream dependencies for one job."""
        return [
            dep
            for dep in _job_dependencies(job_obj)
            if dep in self._failed_lookup or dep in self._skipped_lookup
        ]

    def record_skipped(
        self,
        job_name: str,
        *,
        blocked_by: list[str],
    ) -> None:
        """Record a skipped job caused by failed upstream dependencies."""
        self.skipped_job_names.append(job_name)
        self._skipped_lookup.add(job_name)
        self.executed_jobs.append(
            {
                'job': job_name,
                'reason': 'upstream_failed',
                'skipped_due_to': blocked_by,
                'status': 'skipped',
            },
        )
        self.final_job_name = job_name
        self.final_result = None
        self.final_result_status = None

    def record_failure(
        self,
        job_name: str,
        *,
        exc: Exception,
        duration_ms: int,
    ) -> None:
        """Record a failed job execution."""
        self.failed_job_names.append(job_name)
        self._failed_lookup.add(job_name)
        self.executed_jobs.append(
            {
                'duration_ms': duration_ms,
                'error_message': str(exc),
                'error_type': type(exc).__name__,
                'job': job_name,
                'status': 'failed',
            },
        )
        self.final_job_name = job_name
        self.final_result = None
        self.final_result_status = None

    def record_success(
        self,
        job_name: str,
        *,
        result: JSONDict,
        duration_ms: int,
    ) -> None:
        """Record a successful job execution."""
        result_status = result.get('status')
        self.succeeded_job_names.append(job_name)
        self.executed_jobs.append(
            {
                'duration_ms': duration_ms,
                'job': job_name,
                'result': result,
                'result_status': result_status,
                'status': 'succeeded',
            },
        )
        self.final_job_name = job_name
        self.final_result = result
        self.final_result_status = (
            result_status if isinstance(result_status, str) else None
        )

    def result(self) -> JSONDict:
        """Return the stable summary payload for the run."""
        summary_status = (
            'success'
            if not self.failed_job_names and not self.skipped_job_names
            else 'partial_success'
            if self.continue_on_fail and self.succeeded_job_names
            else 'failed'
        )

        return {
            'continue_on_fail': self.continue_on_fail,
            'executed_job_count': len(self.failed_job_names)
            + len(self.succeeded_job_names),
            'executed_jobs': self.executed_jobs,
            'failed_job_count': len(self.failed_job_names),
            'failed_jobs': self.failed_job_names,
            'final_job': self.final_job_name,
            'final_result': self.final_result,
            'final_result_status': self.final_result_status,
            'job_count': len(self.ordered_job_names),
            'mode': self.mode,
            'ordered_jobs': self.ordered_job_names,
            'requested_job': self.requested_job,
            'skipped_job_count': len(self.skipped_job_names),
            'skipped_jobs': self.skipped_job_names,
            'status': summary_status,
            'succeeded_job_count': len(self.succeeded_job_names),
            'succeeded_jobs': self.succeeded_job_names,
        }


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
    return index_named_items(
        connectors,
        item_label=f'{label} connector',
    )


def _index_jobs(
    jobs: list[Any],
) -> dict[str, Any]:
    """
    Index configured jobs by name with a helpful error on duplicates.

    Parameters
    ----------
    jobs : list[Any]
        Job-like objects to index.

    Returns
    -------
    dict[str, Any]
        Mapping of job name to job object.

    Raises
    ------
    ValueError
        If duplicate job names are found.
    """
    return index_named_items(
        jobs,
        item_label='job',
    )


def _job_dependencies(
    job_obj: Any,
) -> list[str]:
    """Return normalized dependency names for one job-like object."""
    depends_on = getattr(job_obj, 'depends_on', [])
    if isinstance(depends_on, str):
        return [depends_on]
    if not isinstance(depends_on, (list, tuple, set, frozenset)):
        return []
    return [dep for dep in depends_on if isinstance(dep, str)]


def _merge_file_options(
    *option_sets: object,
) -> dict[str, Any]:
    """Merge connector-level and job-level file options with later wins."""
    return merge_mapping_options(
        *option_sets,
        excluded_keys=frozenset({'path', 'format'}),
    )


def _ordered_job_names(
    jobs: list[Any],
) -> list[str]:
    """Return job names in topological order."""
    dag_jobs = [
        _DagJobRef(
            name=name,
            depends_on=_job_dependencies(job),
        )
        for job in jobs
        if isinstance(name := getattr(job, 'name', None), str) and name
    ]
    ordered = topological_sort_jobs(dag_jobs)  # type: ignore[arg-type]
    return [job.name for job in ordered]


def _selected_job_names(
    jobs_by_name: Mapping[str, Any],
    job_name: str,
) -> set[str]:
    """Return the selected job plus its full dependency closure."""
    selected: set[str] = set()
    pending = [job_name]

    while pending:
        current = pending.pop()
        if current in selected:
            continue
        selected.add(current)
        pending.extend(_job_dependencies(jobs_by_name[current]))

    return selected


def _planned_jobs(
    cfg: Any,
    *,
    job_name: str | None,
    run_all: bool,
) -> list[Any]:
    """
    Return jobs to execute in DAG order for the requested run mode.

    Parameters
    ----------
    cfg : Any
        Loaded pipeline configuration.
    job_name : str | None
        Optional requested job name.
    run_all : bool
        Whether to run every configured job.

    Returns
    -------
    list[Any]
        Ordered list of job-like objects to execute.

    Raises
    ------
    ValueError
        If the requested job is not found or if there are configuration issues.
    """
    jobs = list(getattr(cfg, 'jobs', []) or [])
    jobs_by_name = _index_jobs(jobs)
    if not jobs_by_name:
        raise ValueError('No jobs configured')

    ordered_names = _ordered_job_names(jobs)

    if run_all:
        return [jobs_by_name[name] for name in ordered_names]
    if job_name is None:
        raise ValueError('job is required unless run_all is True')
    if job_name not in jobs_by_name:
        raise ValueError(f'Job not found: {job_name}')

    selected_names = _selected_job_names(jobs_by_name, job_name)
    return [jobs_by_name[name] for name in ordered_names if name in selected_names]


def _run_job_config(
    context: _RunContext,
    job_obj: Any,
) -> JSONDict:
    """Execute one configured job object against an already-loaded config."""
    data = _extract_job_data(context, job_obj)
    validation = _JobValidationConfig.from_job(job_obj, context.cfg)
    data = validation.apply(data, when='before_transform')

    if (ops := _resolve_transform_ops(context.cfg, job_obj)) is not None:
        data = transform(data, ops)

    data = validation.apply(data, when='after_transform')
    return _load_job_result(context, job_obj, data)


def _extract_job_data(
    context: _RunContext,
    job_obj: Any,
) -> JSONData:
    """Extract the source payload for one configured job."""
    if not (extract_cfg := getattr(job_obj, 'extract', None)):
        raise ValueError('Job missing "extract" section')

    source_obj = _require_named_connector(
        context.sources_by_name,
        extract_cfg.source,
        label='source',
    )
    overrides = getattr(extract_cfg, 'options', None) or {}

    match DataConnectorType.coerce(getattr(source_obj, 'type', '') or ''):
        case DataConnectorType.FILE:
            file_cfg = _resolve_file_connector_config(
                source_obj,
                overrides,
                missing_path_message='File source missing "path"',
            )
            return extract(
                'file',
                file_cfg.path,
                file_format=file_cfg.file_format,
                **file_cfg.options,
            )
        case DataConnectorType.DATABASE:
            return extract(
                'database',
                getattr(source_obj, 'connection_string', ''),
            )
        case DataConnectorType.API:
            return extract_from_api_source(
                context.cfg,
                source_obj,
                overrides,
            )
        case _:
            raise ValueError(
                f'Unsupported source type: {getattr(source_obj, "type", None)}',
            )


def _load_job_result(
    context: _RunContext,
    job_obj: Any,
    data: JSONData,
) -> JSONDict:
    """Load one job payload into its configured target."""
    if not (load_cfg := getattr(job_obj, 'load', None)):
        raise ValueError('Job missing "load" section')

    target_obj = _require_named_connector(
        context.targets_by_name,
        load_cfg.target,
        label='target',
    )
    overrides = getattr(load_cfg, 'overrides', None) or {}

    match DataConnectorType.coerce(getattr(target_obj, 'type', '') or ''):
        case DataConnectorType.FILE:
            file_cfg = _resolve_file_connector_config(
                target_obj,
                overrides,
                missing_path_message='File target missing "path"',
            )
            result = load(
                data,
                'file',
                file_cfg.path,
                file_format=file_cfg.file_format,
                **file_cfg.options,
            )
        case DataConnectorType.API:
            result = load_to_api_target(
                context.cfg,
                target_obj,
                overrides,
                data,
            )
        case DataConnectorType.DATABASE:
            result = load(
                data,
                'database',
                str(
                    overrides.get('connection_string')
                    or getattr(target_obj, 'connection_string', ''),
                ),
            )
        case _:
            raise ValueError(
                f'Unsupported target type: {getattr(target_obj, "type", None)}',
            )

    if not isinstance(result, dict):
        raise TypeError('load result must be a mapping')
    return result


def _resolve_file_connector_config(
    connector_obj: Any,
    overrides: Mapping[str, Any],
    *,
    missing_path_message: str,
) -> _FileConnectorConfig:
    """Resolve path, format, and merged options for one file connector."""
    path = overrides.get('path') or getattr(connector_obj, 'path', None)
    if not path:
        raise ValueError(missing_path_message)

    return _FileConnectorConfig(
        path=_require_path_like(
            path,
            message=missing_path_message,
        ),
        file_format=overrides.get('format')
        or getattr(
            connector_obj,
            'format',
            'json',
        ),
        options=_merge_file_options(
            getattr(connector_obj, 'options', None),
            overrides,
        ),
    )


def _resolve_transform_ops(
    cfg: Any,
    job_obj: Any,
) -> Any | None:
    """Return transform operations for a job when a transform registry exists."""
    if not (transform_cfg := getattr(job_obj, 'transform', None)):
        return None

    transforms = getattr(cfg, 'transforms', {})
    if transforms is None or not isinstance(transforms, Mapping):
        return None
    return transforms.get(getattr(transform_cfg, 'pipeline', None), {})


def _run_job_plan(
    context: _RunContext,
    jobs: list[Any],
    *,
    requested_job: str | None,
    continue_on_fail: bool,
    mode: str,
) -> JSONDict:
    """Execute multiple jobs in DAG order and return a stable summary."""
    tracker = _RunPlanTracker(
        ordered_job_names=[_require_job_name(job) for job in jobs],
        requested_job=requested_job,
        continue_on_fail=continue_on_fail,
        mode=mode,
    )

    for job_obj in jobs:
        job_name = _require_job_name(job_obj)
        blocked_by = tracker.blocked_dependencies(job_obj)
        if blocked_by:
            tracker.record_skipped(job_name, blocked_by=blocked_by)
            continue

        started_perf = perf_counter()
        try:
            result = _run_job_config(
                context,
                job_obj,
            )
        except (KeyError, OSError, RuntimeError, ValueError) as exc:
            tracker.record_failure(
                job_name,
                exc=exc,
                duration_ms=_duration_ms(started_perf),
            )
            if not continue_on_fail:
                break
            continue

        tracker.record_success(
            job_name,
            result=result,
            duration_ms=_duration_ms(started_perf),
        )
    return tracker.result()


def _require_named_connector(
    connectors: Mapping[str, Any],
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


def _require_job_name(
    job_obj: Any,
) -> str:
    """Return a required non-empty job name."""
    if isinstance(name := getattr(job_obj, 'name', None), str) and name:
        return name
    raise ValueError('Configured job missing "name"')


def _require_path_like(
    value: object,
    *,
    message: str,
) -> StrPath:
    """Return a path/URI string or :class:`Path`, else raise :class:`TypeError`."""
    if isinstance(value, (str, Path)):
        return value
    raise TypeError(message)


def _duration_ms(
    started_perf: float,
) -> int:
    """Convert a perf-counter start time into elapsed milliseconds."""
    return int((perf_counter() - started_perf) * 1000)


def _validate_payload(
    payload: Any,
    rules: Mapping[str, Any],
) -> ValidationResultDict:
    """
    Adapt :func:`etlplus.ops.validate.validate` to the generic callback shape.

    The orchestration layer carries validation rules as loose mappings, while
    :func:`validate` expects field-rule mappings. This adapter localizes that
    type narrowing for static analysis without weakening the public validator
    contract.
    """
    return cast(
        ValidationResultDict,
        validate(
            payload,
            cast(Mapping[str, FieldRulesDict], rules),
        ),
    )


# SECTION: FUNCTIONS ======================================================== #


def run(
    job: str | None = None,
    config_path: str | None = None,
    *,
    run_all: bool = False,
    continue_on_fail: bool = False,
) -> JSONDict:
    """
    Run one configured job or a DAG-ordered job set from a YAML configuration.

    By default it reads the configuration from ``in/pipeline.yml``, but callers
    can provide an explicit *config_path* to override this.

    Parameters
    ----------
    job : str | None, optional
        Job name to execute. When the selected job depends on other jobs, the
        full dependency closure is executed in DAG order. Defaults to
        ``None``.
    config_path : str | None, optional
        Path to the pipeline YAML configuration. Defaults to
        ``in/pipeline.yml``.
    run_all : bool, optional
        Whether to execute all configured jobs in DAG order. Defaults to
        ``False``.
    continue_on_fail : bool, optional
        Whether DAG-style runs should continue past failed jobs and skip only
        blocked downstream jobs. Defaults to ``False``.

    Returns
    -------
    JSONDict
        Result dictionary. Single independent jobs return their terminal load
        result directly; DAG-style runs return an execution summary.
    """
    cfg_path = config_path or DEFAULT_CONFIG_PATH
    cfg = Config.from_yaml(cfg_path, substitute=True)
    planned_jobs = _planned_jobs(
        cfg,
        job_name=job,
        run_all=run_all,
    )
    context = _RunContext.from_config(cfg)

    if len(planned_jobs) > 1 or run_all:
        return _run_job_plan(
            context,
            planned_jobs,
            requested_job=job,
            continue_on_fail=continue_on_fail,
            mode='all' if run_all else 'job',
        )

    return _run_job_config(context, planned_jobs[0])


def run_pipeline(
    *,
    source_type: DataConnectorType | str | None = None,
    source: StrPath | JSONData | None = None,
    operations: PipelineConfig | None = None,
    target_type: DataConnectorType | str | None = None,
    target: StrPath | None = None,
    file_format: FileFormatArg = None,
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
            _require_path_like(
                source,
                message=(
                    'source must be a path-like string or Path when source_type is set'
                ),
            ),
            file_format=file_format,
            **kwargs,
        )

    if operations:
        data = transform(data, operations)

    if target_type is None:
        if not isinstance(data, (dict, list)):
            raise TypeError(
                f'Expected data to be dict or list of dicts, got {type(data).__name__}',
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
