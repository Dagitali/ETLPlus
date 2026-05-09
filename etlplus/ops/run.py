"""
:mod:`etlplus.ops.run` module.

A module for running ETL jobs defined in YAML configurations.
"""

from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from pathlib import Path
from time import perf_counter
from time import sleep
from typing import Any
from typing import Final
from typing import Self
from typing import cast
from typing import overload

from .._config import Config
from ..api import HttpMethod
from ..connector import DataConnectorType
from ..file._core import FileFormatArg
from ..utils import FloatParser
from ..utils import IntParser
from ..utils import JsonCodec
from ..utils import MappingParser
from ..utils._types import JSONData
from ..utils._types import JSONDict
from ..utils._types import StrPath
from ..workflow import topological_sort_jobs
from ._types import DataSourceArg
from ._types import OptionalConnectorTypeArg
from ._types import OptionalPathArg
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


# SECTION: INTERNALCONSTANTS ================================================ #


_JOB_EXECUTION_EXCEPTIONS: Final[tuple[type[Exception], ...]] = (
    KeyError,
    OSError,
    RuntimeError,
    ValueError,
)


# SECTION: CONSTANTS ======================================================== #


DEFAULT_CONFIG_PATH: Final[str] = 'in/pipeline.yml'


# SECTION: INTERNAL DATA CLASSES ============================================ #


@dataclass(frozen=True, slots=True)
class _DagJobRef:
    """
    Minimal job view used for DAG ordering across tolerant job-like inputs.
    """

    # -- Instance Attributes -- #

    name: str
    depends_on: list[str]


@dataclass(frozen=True, slots=True)
class _FileConnectorConfig:
    """Resolved file connector settings for one extract/load step."""

    # -- Instance Attributes -- #

    path: StrPath
    file_format: FileFormatArg
    options: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _JobExecutionOutcome:
    """Terminal outcome for one job execution with retry metadata."""

    # -- Instance Attributes -- #

    started_at: str
    finished_at: str
    duration_ms: int
    result: JSONDict | None = None
    exc: Exception | None = None
    retry_summary: JSONDict | None = None


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
            print_json_fn=JsonCodec(pretty=True).print,
        )


@dataclass(frozen=True, slots=True)
class _ResolvedJobConnector:
    """Resolved connector dispatch settings for one job edge."""

    # -- Instance Attributes -- #

    connector_type: OptionalConnectorTypeArg
    value: StrPath
    file_format: FileFormatArg
    options: dict[str, Any]
    connector_obj: Any


@dataclass(frozen=True, slots=True)
class _ResolvedJobRetry:
    """Normalized retry controls for one job execution."""

    # -- Instance Attributes -- #

    max_attempts: int = 1
    backoff_seconds: float = 0.0

    # -- Instance Properties -- #

    @property
    def enabled(
        self,
    ) -> bool:
        """Return whether retries are enabled beyond the first attempt."""
        return self.max_attempts > 1


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


@dataclass(slots=True)
class _RunPlanTracker:
    """Accumulate execution state for one DAG-style run."""

    # -- Instance Attributes -- #

    ordered_job_names: list[str]
    requested_job: str | None
    continue_on_fail: bool
    mode: str
    max_concurrency: int = 1

    # -- Internal Instance Attributes -- #

    _executed_lookup: dict[int, dict[str, Any]] = field(
        default_factory=dict,
        repr=False,
    )
    _failed_lookup: set[str] = field(default_factory=set, repr=False)
    _skipped_lookup: set[str] = field(default_factory=set, repr=False)

    # -- Getters -- #

    @property
    def executed_jobs(
        self,
    ) -> list[dict[str, Any]]:
        """Return executed-job rows in deterministic DAG order."""
        return [self._executed_lookup[index] for index in sorted(self._executed_lookup)]

    # -- Internal Instance Attributes -- #

    def _job_names_with_status(
        self,
        executed_jobs: list[dict[str, Any]],
        status: str,
    ) -> list[str]:
        """Return recorded job names matching one terminal status."""
        return [
            cast(str, item['job'])
            for item in executed_jobs
            if item.get('status') == status and isinstance(item.get('job'), str)
        ]

    def _last_mapping_value(
        self,
        executed_jobs: list[dict[str, Any]],
        key: str,
    ) -> JSONDict | None:
        """Return the last mapping value stored under *key*."""
        for item in reversed(executed_jobs):
            value = item.get(key)
            if isinstance(value, Mapping):
                return cast(JSONDict, value)
        return None

    def _last_text_value(
        self,
        executed_jobs: list[dict[str, Any]],
        key: str,
    ) -> str | None:
        """Return the last non-empty string value stored under *key*."""
        for item in reversed(executed_jobs):
            value = item.get(key)
            if isinstance(value, str) and value:
                return value
        return None

    def _retry_stats(
        self,
        executed_jobs: list[dict[str, Any]],
    ) -> tuple[list[str], int]:
        """Return retried job names and aggregate retry count."""
        retried_jobs: list[str] = []
        total_retry_count = 0

        for item in executed_jobs:
            job_name = item.get('job')
            retry_summary = item.get('retry')
            if not isinstance(job_name, str) or not isinstance(
                retry_summary,
                Mapping,
            ):
                continue

            attempt_count = retry_summary.get('attempt_count')
            if isinstance(attempt_count, int) and attempt_count > 1:
                retried_jobs.append(job_name)
                total_retry_count += attempt_count - 1

        return retried_jobs, total_retry_count

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
        sequence_index: int,
        timestamp: str,
    ) -> None:
        """Record a skipped job caused by failed upstream dependencies."""
        self._skipped_lookup.add(job_name)
        self._executed_lookup[sequence_index] = {
            'duration_ms': 0,
            'finished_at': timestamp,
            'job': job_name,
            'reason': 'upstream_failed',
            'sequence_index': sequence_index,
            'skipped_due_to': blocked_by,
            'started_at': timestamp,
            'status': 'skipped',
        }

    def record_failure(
        self,
        job_name: str,
        *,
        exc: Exception,
        duration_ms: int,
        finished_at: str,
        retry_summary: JSONDict | None,
        sequence_index: int,
        started_at: str,
    ) -> None:
        """Record a failed job execution."""
        self._failed_lookup.add(job_name)
        job_record: dict[str, Any] = {
            'duration_ms': duration_ms,
            'error_message': str(exc),
            'error_type': type(exc).__name__,
            'finished_at': finished_at,
            'job': job_name,
            'sequence_index': sequence_index,
            'started_at': started_at,
            'status': 'failed',
        }
        if retry_summary is not None:
            job_record['retry'] = retry_summary
        self._executed_lookup[sequence_index] = job_record

    def record_success(
        self,
        job_name: str,
        *,
        result: JSONDict,
        duration_ms: int,
        finished_at: str,
        retry_summary: JSONDict | None,
        sequence_index: int,
        started_at: str,
    ) -> None:
        """Record a successful job execution."""
        result_status = result.get('status')
        job_record: dict[str, Any] = {
            'duration_ms': duration_ms,
            'finished_at': finished_at,
            'job': job_name,
            'result': result,
            'result_status': result_status,
            'sequence_index': sequence_index,
            'started_at': started_at,
            'status': 'succeeded',
        }
        if retry_summary is not None:
            job_record['retry'] = retry_summary
        self._executed_lookup[sequence_index] = job_record

    def result(self) -> JSONDict:
        """Return the stable summary payload for the run."""
        executed_jobs = self.executed_jobs
        failed_job_names = self._job_names_with_status(executed_jobs, 'failed')
        skipped_job_names = self._job_names_with_status(executed_jobs, 'skipped')
        succeeded_job_names = self._job_names_with_status(executed_jobs, 'succeeded')
        retried_jobs, total_retry_count = self._retry_stats(executed_jobs)

        summary_status = (
            'success'
            if not failed_job_names and not skipped_job_names
            else 'partial_success'
            if self.continue_on_fail and succeeded_job_names
            else 'failed'
        )

        summary: JSONDict = {
            'continue_on_fail': self.continue_on_fail,
            'executed_job_count': len(failed_job_names) + len(succeeded_job_names),
            'executed_jobs': executed_jobs,
            'failed_job_count': len(failed_job_names),
            'failed_jobs': failed_job_names,
            'final_job': self._last_text_value(executed_jobs, 'job'),
            'final_result': self._last_mapping_value(executed_jobs, 'result'),
            'final_result_status': self._last_text_value(
                executed_jobs,
                'result_status',
            ),
            'job_count': len(self.ordered_job_names),
            'mode': self.mode,
            'ordered_jobs': self.ordered_job_names,
            'requested_job': self.requested_job,
            'skipped_job_count': len(skipped_job_names),
            'skipped_jobs': skipped_job_names,
            'status': summary_status,
            'succeeded_job_count': len(succeeded_job_names),
            'succeeded_jobs': succeeded_job_names,
        }
        if self.max_concurrency > 1:
            summary['max_concurrency'] = self.max_concurrency
        if retried_jobs:
            summary['retried_job_count'] = len(retried_jobs)
            summary['retried_jobs'] = retried_jobs
            summary['total_attempt_count'] = (
                summary['executed_job_count'] + total_retry_count
            )
            summary['total_retry_count'] = total_retry_count
        return summary


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
    """
    return MappingParser.index_named_items(
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
    """
    return MappingParser.index_named_items(jobs, item_label='job')


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


def _job_retry_value(
    retry_obj: object,
    field_name: str,
) -> object:
    """Return one retry field from a mapping-like or object payload."""
    if isinstance(retry_obj, Mapping):
        return retry_obj.get(field_name)
    return getattr(retry_obj, field_name, None)


def _job_retry_settings(
    job_obj: Any,
) -> _ResolvedJobRetry:
    """Return normalized retry settings for one job-like object."""
    retry_obj = getattr(job_obj, 'retry', None)
    if retry_obj is None:
        return _ResolvedJobRetry()
    return _ResolvedJobRetry(
        max_attempts=IntParser.positive(
            _job_retry_value(retry_obj, 'max_attempts'),
            default=1,
        ),
        backoff_seconds=(
            FloatParser.parse(
                _job_retry_value(retry_obj, 'backoff_seconds'),
                default=0.0,
                minimum=0.0,
            )
            or 0.0
        ),
    )


def _job_retry_summary(
    *,
    attempts: list[JSONDict],
    retry: _ResolvedJobRetry,
) -> JSONDict | None:
    """Return one additive retry summary when retries are configured."""
    if not retry.enabled:
        return None
    return {
        'attempt_count': len(attempts),
        'attempts': attempts,
        'backoff_seconds': retry.backoff_seconds,
        'max_attempts': retry.max_attempts,
        'retried': len(attempts) > 1,
    }


def _maybe_sleep_for_retry(
    backoff_seconds: float,
) -> None:
    """Sleep for one retry backoff interval when configured."""
    if backoff_seconds > 0:
        sleep(backoff_seconds)


def _execute_job_with_retries(
    context: _RunContext,
    job_obj: Any,
) -> _JobExecutionOutcome:
    """Execute one job with optional retry attempts and additive metadata."""
    retry = _job_retry_settings(job_obj)
    total_started_perf = perf_counter()
    job_started_at = _utc_now_iso()
    attempts: list[JSONDict] = []

    for attempt_number in range(1, retry.max_attempts + 1):
        attempt_started_at = _utc_now_iso()
        attempt_started_perf = perf_counter()
        try:
            result = _run_job_config(context, job_obj)
        except _JOB_EXECUTION_EXCEPTIONS as exc:
            attempt_finished_at = _utc_now_iso()
            will_retry = attempt_number < retry.max_attempts
            attempts.append(
                {
                    'attempt': attempt_number,
                    'duration_ms': _duration_ms(attempt_started_perf),
                    'error_message': str(exc),
                    'error_type': type(exc).__name__,
                    'finished_at': attempt_finished_at,
                    'started_at': attempt_started_at,
                    'status': 'failed',
                    'will_retry': will_retry,
                },
            )
            if will_retry:
                _maybe_sleep_for_retry(retry.backoff_seconds)
                continue
            return _JobExecutionOutcome(
                started_at=job_started_at,
                finished_at=attempt_finished_at,
                duration_ms=_duration_ms(total_started_perf),
                exc=exc,
                retry_summary=_job_retry_summary(attempts=attempts, retry=retry),
            )

        attempt_finished_at = _utc_now_iso()
        result_status = result.get('status') if isinstance(result, Mapping) else None
        attempt_payload: JSONDict = {
            'attempt': attempt_number,
            'duration_ms': _duration_ms(attempt_started_perf),
            'finished_at': attempt_finished_at,
            'started_at': attempt_started_at,
            'status': 'succeeded',
        }
        if isinstance(result_status, str):
            attempt_payload['result_status'] = result_status
        attempts.append(attempt_payload)
        return _JobExecutionOutcome(
            started_at=job_started_at,
            finished_at=attempt_finished_at,
            duration_ms=_duration_ms(total_started_perf),
            result=result,
            retry_summary=_job_retry_summary(attempts=attempts, retry=retry),
        )

    raise RuntimeError('job execution reached an unreachable retry state')


def _merge_file_options(
    *option_sets: object,
) -> dict[str, Any]:
    """Merge connector-level and job-level file options with later wins."""
    return MappingParser.merge_to_dict(
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
    data = _apply_operations(
        data,
        _resolve_transform_ops(context.cfg, job_obj),
    )
    data = validation.apply(data, when='after_transform')
    return _load_job_result(context, job_obj, data)


def _extract_job_data(
    context: _RunContext,
    job_obj: Any,
) -> JSONData:
    """Extract the source payload for one configured job."""
    if not (extract_cfg := getattr(job_obj, 'extract', None)):
        raise ValueError('Job missing "extract" section')

    source = _resolve_job_connector(
        context.sources_by_name,
        ref_name=extract_cfg.source,
        label='source',
        overrides=getattr(extract_cfg, 'options', None),
        missing_path_message='File source missing "path"',
    )
    return _dispatch_extract(
        source.connector_type,
        source.value,
        file_format=source.file_format,
        options=source.options,
        cfg=context.cfg,
        connector_obj=source.connector_obj,
    )


def _load_job_result(
    context: _RunContext,
    job_obj: Any,
    data: JSONData,
) -> JSONDict:
    """Load one job payload into its configured target."""
    if not (load_cfg := getattr(job_obj, 'load', None)):
        raise ValueError('Job missing "load" section')

    target = _resolve_job_connector(
        context.targets_by_name,
        ref_name=load_cfg.target,
        label='target',
        overrides=getattr(load_cfg, 'overrides', None),
        missing_path_message='File target missing "path"',
    )
    result = _dispatch_load(
        data,
        target.connector_type,
        target.value,
        file_format=target.file_format,
        options=target.options,
        cfg=context.cfg,
        connector_obj=target.connector_obj,
    )

    if not isinstance(result, dict):
        raise TypeError('load result must be a mapping')
    return result


def _dispatch_extract(
    source_type: OptionalConnectorTypeArg,
    source: StrPath,
    *,
    file_format: FileFormatArg = None,
    options: Mapping[str, Any] | None = None,
    cfg: Any | None = None,
    connector_obj: Any | None = None,
) -> JSONData:
    """Dispatch one extract request through the extract module boundary."""
    resolved_options = dict(options or {})

    match DataConnectorType.coerce(source_type or ''):
        case DataConnectorType.FILE:
            return extract(
                DataConnectorType.FILE,
                source,
                file_format=file_format,
                **resolved_options,
            )
        case DataConnectorType.DATABASE:
            return extract(
                DataConnectorType.DATABASE,
                str(source),
            )
        case DataConnectorType.API:
            if cfg is not None and connector_obj is not None:
                return extract_from_api_source(
                    cfg,
                    connector_obj,
                    resolved_options,
                )
            return extract(
                DataConnectorType.API,
                str(source),
                **resolved_options,
            )
        case _:
            raise ValueError(f'Unsupported source type: {source_type}')


def _dispatch_load(
    data: DataSourceArg,
    target_type: OptionalConnectorTypeArg,
    target: StrPath,
    *,
    file_format: FileFormatArg = None,
    method: HttpMethod | str | None = None,
    options: Mapping[str, Any] | None = None,
    cfg: Any | None = None,
    connector_obj: Any | None = None,
) -> JSONData:
    """Dispatch one load request through the load module boundary."""
    resolved_options = dict(options or {})

    match DataConnectorType.coerce(target_type or ''):
        case DataConnectorType.FILE:
            return load(
                data,
                DataConnectorType.FILE,
                target,
                file_format=file_format,
                **resolved_options,
            )
        case DataConnectorType.DATABASE:
            return load(
                data,
                DataConnectorType.DATABASE,
                str(target),
            )
        case DataConnectorType.API:
            if cfg is not None and connector_obj is not None:
                return load_to_api_target(
                    cfg,
                    connector_obj,
                    resolved_options,
                    cast(JSONData, data),
                )
            return load(
                data,
                DataConnectorType.API,
                target,
                method=method,
                **resolved_options,
            )
        case _:
            raise ValueError(f'Unsupported target type: {target_type}')


def _is_file_connector_type(
    connector_type: object,
) -> bool:
    """Return True when a connector type represents file-based IO."""
    return connector_type in {DataConnectorType.FILE, DataConnectorType.FILE.value}


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


def _resolve_job_connector(
    connectors: Mapping[str, Any],
    *,
    ref_name: str,
    label: str,
    overrides: Mapping[str, Any] | None,
    missing_path_message: str,
) -> _ResolvedJobConnector:
    """Resolve dispatch-ready connector settings for one named job edge."""
    connector_obj = _require_named_connector(
        connectors,
        ref_name,
        label=label,
    )
    connector_type = getattr(connector_obj, 'type', None)
    resolved_overrides = dict(overrides or {})

    if _is_file_connector_type(connector_type):
        file_cfg = _resolve_file_connector_config(
            connector_obj,
            resolved_overrides,
            missing_path_message=missing_path_message,
        )
        return _ResolvedJobConnector(
            connector_type=connector_type,
            value=file_cfg.path,
            file_format=file_cfg.file_format,
            options=file_cfg.options,
            connector_obj=connector_obj,
        )

    connection_value = str(
        resolved_overrides.get('connection_string')
        or getattr(connector_obj, 'connection_string', ''),
    )

    return _ResolvedJobConnector(
        connector_type=connector_type,
        value=connection_value,
        file_format=None,
        options=resolved_overrides,
        connector_obj=connector_obj,
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


@overload
def _apply_operations(
    data: JSONData,
    operations: PipelineConfig | None,
) -> JSONData: ...


@overload
def _apply_operations(
    data: DataSourceArg,
    operations: PipelineConfig | None,
) -> DataSourceArg: ...


def _apply_operations(
    data: DataSourceArg,
    operations: PipelineConfig | None,
) -> DataSourceArg:
    """Apply configured transform operations, preserving absent transforms."""
    if operations is None:
        return data
    return transform(data, operations)


def _duration_ms(
    started_perf: float,
) -> int:
    """Convert a perf-counter start time into elapsed milliseconds."""
    return int((perf_counter() - started_perf) * 1000)


def _resolved_max_concurrency(
    max_concurrency: object,
) -> int:
    """Return one bounded concurrency setting with a serial default."""
    return IntParser.positive(max_concurrency, default=1)


def _refresh_ready_jobs(
    *,
    allow_scheduling: bool,
    completed_job_names: set[str],
    jobs_by_name: Mapping[str, Any],
    ordered_job_names: list[str],
    ready_queue: list[str],
    seen_job_names: set[str],
    sequence_lookup: Mapping[str, int],
    tracker: _RunPlanTracker,
) -> None:
    """Update ready and skipped jobs after one or more terminal outcomes."""
    for job_name in ordered_job_names:
        if job_name in seen_job_names or job_name in completed_job_names:
            continue
        job_obj = jobs_by_name[job_name]
        blocked_by = tracker.blocked_dependencies(job_obj)
        if blocked_by:
            tracker.record_skipped(
                job_name,
                blocked_by=blocked_by,
                sequence_index=sequence_lookup[job_name],
                timestamp=_utc_now_iso(),
            )
            completed_job_names.add(job_name)
            seen_job_names.add(job_name)
            continue
        if not allow_scheduling:
            continue
        if all(dep in completed_job_names for dep in _job_dependencies(job_obj)):
            ready_queue.append(job_name)
            seen_job_names.add(job_name)


def _run_job_plan_serial(
    context: _RunContext,
    jobs: list[Any],
    *,
    requested_job: str | None,
    continue_on_fail: bool,
    mode: str,
    max_concurrency: int,
) -> JSONDict:
    """Execute DAG jobs serially and return the stable summary payload."""
    tracker = _RunPlanTracker(
        ordered_job_names=[_require_job_name(job) for job in jobs],
        requested_job=requested_job,
        continue_on_fail=continue_on_fail,
        mode=mode,
        max_concurrency=max_concurrency,
    )

    for job_obj in jobs:
        job_name = _require_job_name(job_obj)
        sequence_index = tracker.ordered_job_names.index(job_name)
        blocked_by = tracker.blocked_dependencies(job_obj)
        if blocked_by:
            tracker.record_skipped(
                job_name,
                blocked_by=blocked_by,
                sequence_index=sequence_index,
                timestamp=_utc_now_iso(),
            )
            continue

        outcome = _execute_job_with_retries(context, job_obj)
        if outcome.exc is not None:
            tracker.record_failure(
                job_name,
                exc=outcome.exc,
                duration_ms=outcome.duration_ms,
                finished_at=outcome.finished_at,
                retry_summary=outcome.retry_summary,
                sequence_index=sequence_index,
                started_at=outcome.started_at,
            )
            if not continue_on_fail:
                break
            continue

        tracker.record_success(
            job_name,
            result=cast(JSONDict, outcome.result),
            duration_ms=outcome.duration_ms,
            finished_at=outcome.finished_at,
            retry_summary=outcome.retry_summary,
            sequence_index=sequence_index,
            started_at=outcome.started_at,
        )
    return tracker.result()


def _run_job_plan_parallel(
    context: _RunContext,
    jobs: list[Any],
    *,
    requested_job: str | None,
    continue_on_fail: bool,
    mode: str,
    max_concurrency: int,
) -> JSONDict:
    """Execute independent DAG jobs concurrently with a bounded worker pool."""
    ordered_job_names = [_require_job_name(job) for job in jobs]
    tracker = _RunPlanTracker(
        ordered_job_names=ordered_job_names,
        requested_job=requested_job,
        continue_on_fail=continue_on_fail,
        mode=mode,
        max_concurrency=max_concurrency,
    )
    jobs_by_name = {_require_job_name(job): job for job in jobs}
    sequence_lookup = {
        job_name: index for index, job_name in enumerate(ordered_job_names)
    }
    completed_job_names: set[str] = set()
    ready_queue: list[str] = []
    seen_job_names: set[str] = set()
    stop_scheduling = False

    _refresh_ready_jobs(
        allow_scheduling=True,
        completed_job_names=completed_job_names,
        jobs_by_name=jobs_by_name,
        ordered_job_names=ordered_job_names,
        ready_queue=ready_queue,
        seen_job_names=seen_job_names,
        sequence_lookup=sequence_lookup,
        tracker=tracker,
    )

    with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
        running: dict[Future[_JobExecutionOutcome], str] = {}

        while ready_queue or running:
            while (
                ready_queue and len(running) < max_concurrency and not stop_scheduling
            ):
                job_name = ready_queue.pop(0)
                running[
                    executor.submit(
                        _execute_job_with_retries,
                        context,
                        jobs_by_name[job_name],
                    )
                ] = job_name

            if not running:
                break

            done, _pending = wait(
                tuple(running),
                return_when=FIRST_COMPLETED,
            )
            for future in sorted(done, key=lambda item: sequence_lookup[running[item]]):
                job_name = running.pop(future)
                sequence_index = sequence_lookup[job_name]
                outcome = future.result()
                completed_job_names.add(job_name)
                if outcome.exc is not None:
                    tracker.record_failure(
                        job_name,
                        exc=outcome.exc,
                        duration_ms=outcome.duration_ms,
                        finished_at=outcome.finished_at,
                        retry_summary=outcome.retry_summary,
                        sequence_index=sequence_index,
                        started_at=outcome.started_at,
                    )
                    if not continue_on_fail:
                        stop_scheduling = True
                    continue

                tracker.record_success(
                    job_name,
                    result=cast(JSONDict, outcome.result),
                    duration_ms=outcome.duration_ms,
                    finished_at=outcome.finished_at,
                    retry_summary=outcome.retry_summary,
                    sequence_index=sequence_index,
                    started_at=outcome.started_at,
                )

            _refresh_ready_jobs(
                allow_scheduling=continue_on_fail or not stop_scheduling,
                completed_job_names=completed_job_names,
                jobs_by_name=jobs_by_name,
                ordered_job_names=ordered_job_names,
                ready_queue=ready_queue,
                seen_job_names=seen_job_names,
                sequence_lookup=sequence_lookup,
                tracker=tracker,
            )

    return tracker.result()


def _require_record_payload(
    data: DataSourceArg,
) -> JSONData:
    """Require a dict/list JSON payload for target-less pipeline runs."""
    if not isinstance(data, (dict, list)):
        raise TypeError(
            f'Expected data to be dict or list of dicts, got {type(data).__name__}',
        )
    return data


def _run_job_plan(
    context: _RunContext,
    jobs: list[Any],
    *,
    requested_job: str | None,
    continue_on_fail: bool,
    mode: str,
    max_concurrency: int = 1,
) -> JSONDict:
    """Execute multiple jobs in DAG order and return a stable summary."""
    if max_concurrency <= 1 or len(jobs) <= 1:
        return _run_job_plan_serial(
            context,
            jobs,
            requested_job=requested_job,
            continue_on_fail=continue_on_fail,
            mode=mode,
            max_concurrency=max_concurrency,
        )
    return _run_job_plan_parallel(
        context,
        jobs,
        requested_job=requested_job,
        continue_on_fail=continue_on_fail,
        mode=mode,
        max_concurrency=max_concurrency,
    )


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
    connectors : Mapping[str, Any]
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


def _utc_now_iso() -> str:
    """Return the current UTC timestamp in ``Z``-suffixed ISO-8601 form."""
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


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
    max_concurrency: int | None = None,
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
    max_concurrency : int | None, optional
        Maximum number of independent DAG jobs to run concurrently for
        DAG-style runs. Defaults to ``None`` which preserves serial
        execution.

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
    resolved_max_concurrency = _resolved_max_concurrency(max_concurrency)

    if len(planned_jobs) > 1 or run_all:
        return _run_job_plan(
            context,
            planned_jobs,
            requested_job=job,
            continue_on_fail=continue_on_fail,
            mode='all' if run_all else 'job',
            max_concurrency=resolved_max_concurrency,
        )

    return _run_job_config(context, planned_jobs[0])


def run_pipeline(
    *,
    source_type: OptionalConnectorTypeArg = None,
    source: DataSourceArg | None = None,
    operations: PipelineConfig | None = None,
    target_type: OptionalConnectorTypeArg = None,
    target: OptionalPathArg = None,
    file_format: FileFormatArg = None,
    method: HttpMethod | str | None = None,
    **kwargs: Any,
) -> JSONData:
    """
    Run a single extract-transform-load flow without a YAML config.

    Parameters
    ----------
    source_type : OptionalConnectorTypeArg, optional
        Connector type for extraction. When ``None``, *source* is assumed
        to be pre-loaded data and extraction is skipped.
    source : DataSourceArg | None, optional
        Data source for extraction or the pre-loaded payload when
        *source_type* is ``None``.
    operations : PipelineConfig | None, optional
        Transform configuration passed to :func:`etlplus.ops.transform`.
    target_type : OptionalConnectorTypeArg, optional
        Connector type for loading. When ``None``, load is skipped and the
        transformed data is returned.
    target : OptionalPathArg, optional
        Target for loading (file path, connection string, or API URL).
    file_format : FileFormatArg, optional
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
        data = _dispatch_extract(
            source_type,
            _require_path_like(
                source,
                message=(
                    'source must be a path-like string or Path when source_type is set'
                ),
            ),
            file_format=file_format,
            options=kwargs,
        )

    data = _apply_operations(data, operations)

    if target_type is None:
        return _require_record_payload(data)
    if target is None:
        raise ValueError('target is required when target_type is set')

    return _dispatch_load(
        data,
        target_type,
        target,
        file_format=file_format,
        method=method,
        options=kwargs,
    )
