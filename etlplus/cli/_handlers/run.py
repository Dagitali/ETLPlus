"""
:mod:`etlplus.cli._handlers.run` module.

Run-command implementation for the CLI facade.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import cast

from ... import Config
from ... import __version__
from ...history import HistoryStore
from ...history import build_run_record
from ...history._store import JobRunRecord
from ...ops import run
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


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_job_result_summary(
    item: Mapping[str, object],
) -> JSONData | None:
    """Return one JSON-serializable per-job result summary when available."""
    result = item.get('result')
    if isinstance(result, (dict, list, str, int, float, bool)) or result is None:
        if result is not None:
            return cast(JSONData, result)

    reason = item.get('reason')
    skipped_due_to = item.get('skipped_due_to')
    if isinstance(reason, str) or isinstance(skipped_due_to, list):
        summary: dict[str, object] = {}
        if isinstance(reason, str):
            summary['reason'] = reason
        if isinstance(skipped_due_to, list):
            summary['skipped_due_to'] = [
                value for value in skipped_due_to if isinstance(value, str)
            ]
        return cast(JSONData, summary)
    return None


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
        raw_sequence_index
        if isinstance(raw_sequence_index, int)
        else fallback_index
    )
    started_at = raw_started_at if isinstance(raw_started_at, str) else None
    finished_at = raw_finished_at if isinstance(raw_finished_at, str) else None
    duration_ms = raw_duration_ms if isinstance(raw_duration_ms, int) else None
    result_status = (
        raw_result_status if isinstance(raw_result_status, str) else None
    )
    error_type = raw_error_type if isinstance(raw_error_type, str) else None
    error_message = (
        raw_error_message if isinstance(raw_error_message, str) else None
    )
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
    history_store = HistoryStore.from_environment()
    history_store.record_run_started(
        build_run_record(
            run_id=context.run_id,
            config_path=config,
            started_at=context.started_at,
            pipeline_name=cfg.name,
            job_name=None if run_all else job_name,
        ),
    )

    with _lifecycle.failure_boundary(
        context,
        on_error=lambda exc: _lifecycle.record_run_completion(
            history_store,
            context,
            status='failed',
            exc=exc,
        ),
        config_path=config,
        job=job_name,
        pipeline_name=cfg.name,
    ):
        result = run(
            job=job_name,
            config_path=config,
            run_all=run_all,
            continue_on_fail=continue_on_fail,
        )

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
        result_summary=cast(JSONData | None, result),
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
            duration_ms=_lifecycle.elapsed_ms(context.started_perf),
            error_message=_failure_message(result),
            error_type='RunExecutionFailed',
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
