"""
:mod:`etlplus.cli._handlers.run` module.

Run-command handler.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any
from typing import Final
from typing import cast

from ... import Config
from ... import __version__
from ...history import HistoryStore
from ...history import RunRecord
from ...history._config import HistoryConfig
from ...history._config import ResolvedHistoryConfig
from ...history._store import JobRunRecord
from ...ops import run
from ...runtime import RuntimeTelemetry
from ...utils._types import JSONData
from . import _completion
from . import _lifecycle
from . import _output
from . import _summary

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'run_handler',
]


# SECTION: CONSTANTS ======================================================== #


_DAG_SUMMARY_STATUS_FALLBACK: Final[tuple[str, str, str]] = (
    'success',
    'partial_success',
    'failed',
)


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_job_result_summary(
    item: Mapping[str, object],
) -> JSONData | None:
    """Return one JSON-serializable per-job result summary when available."""
    result = item.get('result')
    retry_summary = item.get('retry')
    summary: JSONData | None = None
    if isinstance(result, (dict, list, str, int, float, bool)) or result is None:
        if result is not None:
            summary = cast(JSONData, result)

    reason = item.get('reason')
    skipped_due_to = item.get('skipped_due_to')
    if summary is None and (
        isinstance(reason, str) or isinstance(skipped_due_to, list)
    ):
        fallback_summary: dict[str, object] = {}
        if isinstance(reason, str):
            fallback_summary['reason'] = reason
        if isinstance(skipped_due_to, list):
            fallback_summary['skipped_due_to'] = [
                value for value in skipped_due_to if isinstance(value, str)
            ]
        summary = cast(JSONData, fallback_summary)

    if isinstance(retry_summary, Mapping):
        retry_payload = {
            key: value
            for key, value in retry_summary.items()
            if isinstance(key, str)
            and isinstance(value, (dict, list, str, int, float, bool))
        }
        if retry_payload:
            if isinstance(summary, Mapping):
                merged_summary = dict(summary)
                merged_summary['retry'] = retry_payload
                return cast(JSONData, merged_summary)
            if summary is not None:
                return cast(
                    JSONData,
                    {
                        'result': summary,
                        'retry': retry_payload,
                    },
                )
            return cast(JSONData, {'retry': retry_payload})
    return summary


def _coerce_string_list(
    value: object,
) -> list[str]:
    """Return one filtered string list for persisted summary fields."""
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _dag_run_status(
    result: Mapping[str, object],
    *,
    continue_on_fail: bool,
    failed_jobs: list[str],
    skipped_jobs: list[str],
    succeeded_jobs: list[str],
) -> str:
    """Return one normalized persisted status for a DAG-style run summary."""
    if isinstance(status := result.get('status'), str) and status:
        return status
    if failed_jobs or skipped_jobs:
        return (
            _DAG_SUMMARY_STATUS_FALLBACK[1]
            if continue_on_fail and succeeded_jobs
            else _DAG_SUMMARY_STATUS_FALLBACK[2]
        )
    return _DAG_SUMMARY_STATUS_FALLBACK[0]


def _job_run_record(
    *,
    fallback_index: int,
    item: Mapping[str, object],
    pipeline_name: str | None,
    run_id: str,
) -> JobRunRecord | None:
    """Build one persisted job-run record from a DAG execution summary entry."""
    job_name = item.get('job')
    status = item.get('status')
    if not isinstance(job_name, str) or not job_name:
        return None
    if not isinstance(status, str) or not status:
        return None

    raw_sequence_index = item.get('sequence_index')
    raw_started_at = item.get('started_at')
    raw_finished_at = item.get('finished_at')
    raw_duration_ms = item.get('duration_ms')
    raw_result_status = item.get('result_status')
    raw_error_type = item.get('error_type')
    raw_error_message = item.get('error_message')
    skipped_due_to = item.get('skipped_due_to')
    sequence_index = (
        raw_sequence_index if isinstance(raw_sequence_index, int) else fallback_index
    )
    started_at = raw_started_at if isinstance(raw_started_at, str) else None
    finished_at = raw_finished_at if isinstance(raw_finished_at, str) else None
    duration_ms = raw_duration_ms if isinstance(raw_duration_ms, int) else None
    result_status = raw_result_status if isinstance(raw_result_status, str) else None
    error_type = raw_error_type if isinstance(raw_error_type, str) else None
    error_message = raw_error_message if isinstance(raw_error_message, str) else None
    return JobRunRecord(
        run_id=run_id,
        job_name=job_name,
        pipeline_name=pipeline_name,
        sequence_index=sequence_index,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=duration_ms,
        records_in=None,
        records_out=None,
        status=status,
        result_status=result_status,
        error_type=error_type,
        error_message=error_message,
        skipped_due_to=(
            [value for value in skipped_due_to if isinstance(value, str)]
            if isinstance(skipped_due_to, list)
            else None
        ),
        result_summary=_coerce_job_result_summary(item),
    )


def _last_job_field(
    executed_jobs: list[object],
    *,
    field_name: str,
) -> str | None:
    """Return one trailing string field from the executed-job rows when present."""
    for item in reversed(executed_jobs):
        if not isinstance(item, Mapping):
            continue
        value = item.get(field_name)
        if isinstance(value, str) and value:
            return value
        return None
    return None


def _persist_job_runs(
    history_store: Any,
    *,
    pipeline_name: str | None,
    result: Mapping[str, object] | object,
    run_id: str,
) -> None:
    """Persist per-job DAG records when the run result carries execution rows."""
    if not isinstance(result, Mapping):
        return
    executed_jobs = result.get('executed_jobs')
    if not isinstance(executed_jobs, list):
        return

    for fallback_index, item in enumerate(executed_jobs):
        if not isinstance(item, Mapping):
            continue
        job_run = _job_run_record(
            fallback_index=fallback_index,
            item=item,
            pipeline_name=pipeline_name,
            run_id=run_id,
        )
        if job_run is not None:
            history_store.record_job_run(job_run)
            RuntimeTelemetry.emit_history_record(
                job_run.to_payload(),
                record_level='job',
            )


def _open_history_store(
    settings: ResolvedHistoryConfig,
) -> HistoryStore | None:
    """Return one local history store when persistence is enabled."""
    if not settings.enabled:
        return None
    env_settings = ResolvedHistoryConfig.resolve(None, env=os.environ)
    if (
        settings.backend == env_settings.backend
        and settings.state_dir == env_settings.state_dir
    ):
        return HistoryStore.from_environment()
    return HistoryStore.from_settings(
        backend=settings.backend,
        state_dir=settings.state_dir,
    )


def _persisted_run_summary(
    result: Mapping[str, object] | object,
) -> JSONData | None:
    """
    Return the persisted run summary shape for one CLI run result.

    DAG-style results persist a concise aggregate summary at the run level and
    leave per-job detail to ``job_runs``.
    """
    if result is None:
        return None
    if not isinstance(result, Mapping):
        return cast(JSONData, result)

    executed_jobs = result.get('executed_jobs')
    if not isinstance(executed_jobs, list):
        return cast(JSONData, result)

    ordered_jobs = _coerce_string_list(result.get('ordered_jobs'))
    failed_jobs = _coerce_string_list(result.get('failed_jobs'))
    skipped_jobs = _coerce_string_list(result.get('skipped_jobs'))
    succeeded_jobs = _coerce_string_list(result.get('succeeded_jobs'))
    raw_continue_on_fail = result.get('continue_on_fail')
    continue_on_fail = (
        raw_continue_on_fail
        if isinstance(
            raw_continue_on_fail,
            bool,
        )
        else False
    )

    max_concurrency = result.get('max_concurrency')
    retried_job_count = result.get('retried_job_count')
    retried_jobs = result.get('retried_jobs')
    total_attempt_count = result.get('total_attempt_count')
    total_retry_count = result.get('total_retry_count')

    return cast(
        JSONData,
        {
            'continue_on_fail': continue_on_fail,
            'executed_job_count': (
                result.get('executed_job_count')
                if isinstance(result.get('executed_job_count'), int)
                else len(failed_jobs) + len(succeeded_jobs)
            ),
            'failed_job_count': (
                result.get('failed_job_count')
                if isinstance(result.get('failed_job_count'), int)
                else len(failed_jobs)
            ),
            'failed_jobs': failed_jobs,
            'final_job': (
                result.get('final_job')
                if isinstance(result.get('final_job'), str)
                else _last_job_field(executed_jobs, field_name='job')
            ),
            'final_result_status': (
                result.get('final_result_status')
                if isinstance(result.get('final_result_status'), str)
                else _last_job_field(executed_jobs, field_name='result_status')
            ),
            'job_count': (
                result.get('job_count')
                if isinstance(result.get('job_count'), int)
                else len(ordered_jobs)
            ),
            'mode': (
                result.get('mode') if isinstance(result.get('mode'), str) else 'all'
            ),
            'ordered_jobs': ordered_jobs,
            'requested_job': (
                result.get('requested_job')
                if isinstance(result.get('requested_job'), str)
                else None
            ),
            'skipped_job_count': (
                result.get('skipped_job_count')
                if isinstance(result.get('skipped_job_count'), int)
                else len(skipped_jobs)
            ),
            'skipped_jobs': skipped_jobs,
            'status': _dag_run_status(
                result,
                continue_on_fail=continue_on_fail,
                failed_jobs=failed_jobs,
                skipped_jobs=skipped_jobs,
                succeeded_jobs=succeeded_jobs,
            ),
            'succeeded_job_count': (
                result.get('succeeded_job_count')
                if isinstance(result.get('succeeded_job_count'), int)
                else len(succeeded_jobs)
            ),
            'succeeded_jobs': succeeded_jobs,
            **(
                {
                    'max_concurrency': max_concurrency,
                    'retried_job_count': retried_job_count,
                    'retried_jobs': _coerce_string_list(retried_jobs),
                    'total_attempt_count': total_attempt_count,
                    'total_retry_count': total_retry_count,
                }
                if (
                    isinstance(max_concurrency, int) and max_concurrency > 1
                )
                or (
                    isinstance(retried_job_count, int)
                    and isinstance(total_attempt_count, int)
                    and isinstance(total_retry_count, int)
                )
                else {}
            ),
        },
    )


def _resolved_history_settings(
    config: HistoryConfig | None,
    *,
    history_enabled: bool | None,
    history_backend: str | None,
    history_state_dir: str | None,
    capture_tracebacks: bool | None,
) -> ResolvedHistoryConfig:
    """Return effective history settings for one run command invocation."""
    return ResolvedHistoryConfig.resolve(
        config,
        env=os.environ,
        enabled=history_enabled,
        backend=history_backend,
        state_dir=history_state_dir,
        capture_tracebacks=capture_tracebacks,
    )


def _result_failed(
    result: Mapping[str, object] | object,
) -> bool:
    """Return whether one run result represents a handled execution failure."""
    return isinstance(result, Mapping) and result.get('status') in {
        'failed',
        'partial_success',
    }


# SECTION: FUNCTIONS ======================================================== #


def run_handler(
    *,
    config: str,
    job: str | None = None,
    pipeline: str | None = None,
    run_all: bool = False,
    continue_on_fail: bool = False,
    max_concurrency: int | None = None,
    history_enabled: bool | None = None,
    history_backend: str | None = None,
    history_state_dir: str | None = None,
    capture_tracebacks: bool | None = None,
    event_format: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Execute a configured ETL job or pipeline.

    Parameters
    ----------
    config : str
        Path to the YAML/JSON config file.
    job : str | None, optional
        Job name to run. Default is ``None``.
    pipeline : str | None, optional
        Pipeline name to run when *job* is not provided. Default is ``None``.
    run_all : bool, optional
        Whether to run all configured jobs in DAG order. Default is ``False``.
    continue_on_fail : bool, optional
        Whether to continue past failed jobs and skip only blocked downstream
        jobs. Default is ``False``.
    max_concurrency : int | None, optional
        Maximum number of independent DAG jobs to run concurrently. Default
        is ``None`` which preserves serial execution.
    history_enabled : bool | None, optional
        Override whether local run history should be persisted.
    history_backend : str | None, optional
        Override the configured local history backend.
    history_state_dir : str | None, optional
        Override the configured local history state directory.
    capture_tracebacks : bool | None, optional
        Override whether failure tracebacks should be persisted in local
        history.
    event_format : str | None, optional
        Structured event output format. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    cfg = Config.from_yaml(config, substitute=True)
    RuntimeTelemetry.configure(
        getattr(cfg, 'telemetry', None),
        env=os.environ,
        force=True,
    )
    history_settings = _resolved_history_settings(
        getattr(cfg, 'history', None),
        history_enabled=history_enabled,
        history_backend=history_backend,
        history_state_dir=history_state_dir,
        capture_tracebacks=capture_tracebacks,
    )

    job_name = job or pipeline
    if not job_name and not run_all:
        return _output.emit_json_payload(_summary.pipeline_summary(cfg), pretty=pretty)

    context = _lifecycle.start_command(
        command='run',
        event_format=event_format,
        config_path=config,
        etlplus_version=__version__,
        continue_on_fail=continue_on_fail,
        job=job_name,
        run_all=run_all,
        pipeline_name=cfg.name,
        status='running',
    )
    history_store = _open_history_store(history_settings)
    if history_store is not None:
        history_store.record_run_started(
            RunRecord.build(
                run_id=context.run_id,
                config_path=config,
                started_at=context.started_at,
                pipeline_name=cfg.name,
                job_name=None if run_all else job_name,
            ),
        )

    with _lifecycle.failure_boundary(
        context,
        on_error=(
            None
            if history_store is None
            else lambda exc: _lifecycle.record_run_completion(
                history_store,
                context,
                status='failed',
                pipeline_name=cfg.name,
                job_name=None if run_all else job_name,
                config_path=config,
                etlplus_version=__version__,
                exc=exc,
                capture_tracebacks=history_settings.capture_tracebacks,
            )
        ),
        continue_on_fail=continue_on_fail,
        config_path=config,
        etlplus_version=__version__,
        job=job_name,
        pipeline_name=cfg.name,
        run_all=run_all,
    ):
        result = run(
            job=job_name,
            config_path=config,
            run_all=run_all,
            continue_on_fail=continue_on_fail,
            max_concurrency=max_concurrency,
        )

    if history_store is not None:
        _persist_job_runs(
            history_store,
            run_id=context.run_id,
            pipeline_name=cfg.name,
            result=result,
        )
        _lifecycle.record_run_completion(
            history_store,
            context,
            status='failed' if _result_failed(result) else 'succeeded',
            pipeline_name=cfg.name,
            job_name=None if run_all else job_name,
            config_path=config,
            etlplus_version=__version__,
            result_summary=_persisted_run_summary(result),
            error_message=_failure_message(result),
            error_type='RunExecutionFailed' if _result_failed(result) else None,
        )
    result_status = result.get('status') if isinstance(result, dict) else None
    payload = {
        'run_id': context.run_id,
        'status': 'error' if _result_failed(result) else 'ok',
        'result': result,
    }
    if _result_failed(result):
        _lifecycle.emit_lifecycle_event(
            command=context.command,
            lifecycle='failed',
            run_id=context.run_id,
            event_format=context.event_format,
            continue_on_fail=continue_on_fail,
            config_path=config,
            duration_ms=_lifecycle.elapsed_ms(context.started_perf),
            error_message=_failure_message(result),
            error_type='RunExecutionFailed',
            etlplus_version=__version__,
            job=job_name,
            pipeline_name=cfg.name,
            result_status=result_status,
            run_all=run_all,
            status='error',
        )
        return _output.emit_json_payload(payload, pretty=pretty, exit_code=1)
    return _completion.complete_output(
        context,
        payload,
        mode='json',
        pretty=pretty,
        config_path=config,
        continue_on_fail=continue_on_fail,
        etlplus_version=__version__,
        job=job_name,
        pipeline_name=cfg.name,
        result_status=result_status,
        run_all=run_all,
        status='ok',
    )


def _failure_message(
    result: Mapping[str, object] | object,
) -> str | None:
    """Return one concise failure message for DAG-style run summaries."""
    if not isinstance(result, Mapping):
        return None
    status = result.get('status')
    if status not in {'failed', 'partial_success'}:
        return None
    failed_jobs = result.get('failed_jobs')
    skipped_jobs = result.get('skipped_jobs')
    failed_count = len(failed_jobs) if isinstance(failed_jobs, list) else 0
    skipped_count = len(skipped_jobs) if isinstance(skipped_jobs, list) else 0
    if failed_count == 1 and isinstance(failed_jobs, list):
        return f'Job "{failed_jobs[0]}" failed during DAG execution'
    if failed_count or skipped_count:
        return (
            f'{failed_count} job(s) failed and {skipped_count} job(s) were '
            'skipped during DAG execution'
        )
    return 'DAG execution failed'
