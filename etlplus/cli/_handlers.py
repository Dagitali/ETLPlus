"""
:mod:`etlplus.cli._handlers` module.

Command handler functions for the ``etlplus`` command-line interface (CLI).
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter
from time import sleep
from typing import Any
from typing import Literal
from typing import cast

from .. import Config
from .. import __version__
from ..database import render_tables
from ..file import File
from ..file import FileFormat
from ..history import HistoryStore
from ..history import RunCompletion
from ..history import RunState
from ..history import build_run_record
from ..ops import extract
from ..ops import load
from ..ops import run
from ..ops import transform
from ..ops import validate
from ..runtime import ReadinessReportBuilder
from ..runtime import RuntimeEvents
from ..utils._types import JSONData
from ..utils._types import TemplateKey
from . import _handler_check as _check_impl
from . import _handler_common as _common_impl
from . import _handler_dataops as _dataops_impl
from . import _handler_history as _history_impl
from . import _handler_render as _render_impl
from . import _io
from . import _summary
from ._history import HISTORY_TABLE_COLUMNS as _HISTORY_TABLE_COLUMNS
from ._history import REPORT_TABLE_COLUMNS as _REPORT_TABLE_COLUMNS
from ._history import HistoryReportBuilder
from ._history import HistoryView
from ._summary import check_sections as _check_sections
from ._summary import pipeline_summary as _pipeline_summary

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'check_handler',
    'extract_handler',
    'history_handler',
    'load_handler',
    'render_handler',
    'report_handler',
    'run_handler',
    'status_handler',
    'transform_handler',
    'validate_handler',
]


# SECTION: INTERNAL TYPE ALIASES ============================================ #


type _CompletionMode = Literal['file', 'json', 'json_file', 'or_write']
type _RunCompletionStatus = Literal['failed', 'succeeded']

_CommandContext = _common_impl.CommandContext


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _elapsed_ms(
    started_perf: float,
) -> int:
    """Return elapsed milliseconds since *started_perf*."""
    return _common_impl.elapsed_ms(
        started_perf,
        perf_counter_fn=perf_counter,
    )


def _emit_failure_event(
    *,
    command: str,
    run_id: str,
    started_perf: float,
    event_format: str | None,
    exc: Exception,
    **fields: Any,
) -> None:
    """Emit a failure event with the shared stable schema."""
    _common_impl.emit_failure_event(
        command=command,
        run_id=run_id,
        started_perf=started_perf,
        event_format=event_format,
        exc=exc,
        elapsed_ms_fn=_elapsed_ms,
        emit_lifecycle_event_fn=_emit_lifecycle_event,
        **fields,
    )


def _emit_json_payload(
    payload: Any,
    *,
    pretty: bool,
    exit_code: int = 0,
) -> int:
    """Emit one JSON payload and return the requested exit code."""
    return _common_impl.emit_json_payload(
        payload,
        pretty=pretty,
        io_module=_io,
        exit_code=exit_code,
    )


def _emit_lifecycle_event(
    *,
    command: str,
    lifecycle: str,
    run_id: str,
    event_format: str | None,
    **fields: Any,
) -> None:
    """Emit one structured command lifecycle event."""
    _common_impl.emit_lifecycle_event(
        command=command,
        lifecycle=lifecycle,
        run_id=run_id,
        event_format=event_format,
        runtime_events=RuntimeEvents,
        **fields,
    )


def _emit_history_payload(
    payload: Any,
    *,
    columns: tuple[str, ...],
    pretty: bool,
    table: bool = False,
    json_output: bool = False,
    table_rows: list[dict[str, Any]] | None = None,
    exit_code: int = 0,
) -> int:
    """Validate history output mode and emit one JSON or table payload."""
    return _common_impl.emit_history_payload(
        payload,
        columns=columns,
        pretty=pretty,
        history_view=HistoryView,
        emit_json_payload_fn=_emit_json_payload,
        io_module=_io,
        table=table,
        json_output=json_output,
        table_rows=table_rows,
        exit_code=exit_code,
    )


def _complete_command(
    context: _CommandContext,
    **fields: Any,
) -> None:
    """Emit a completed lifecycle event for one command context."""
    _emit_lifecycle_event(
        command=context.command,
        lifecycle='completed',
        run_id=context.run_id,
        event_format=context.event_format,
        duration_ms=_elapsed_ms(context.started_perf),
        **fields,
    )


def _fail_command(
    context: _CommandContext,
    exc: Exception,
    **fields: Any,
) -> None:
    """Emit a failed lifecycle event for one command context."""
    _emit_failure_event(
        command=context.command,
        run_id=context.run_id,
        started_perf=context.started_perf,
        event_format=context.event_format,
        exc=exc,
        **fields,
    )


@contextmanager
def _failure_boundary(
    context: _CommandContext,
    *,
    on_error: Callable[[Exception], None] | None = None,
    **fields: Any,
) -> Iterator[None]:
    """Emit a failed lifecycle event for exceptions raised inside the block."""
    try:
        yield
    except Exception as exc:
        if on_error is not None:
            on_error(exc)
        _fail_command(context, exc, **fields)
        raise


def _load_history_records(
    *,
    raw: bool = False,
    job: str | None = None,
    limit: int | None = None,
    run_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    """Load, filter, and sort history records for CLI read commands."""
    return _history_impl.load_history_records(
        history_view=HistoryView,
        raw=raw,
        job=job,
        limit=limit,
        run_id=run_id,
        since=since,
        until=until,
        status=status,
    )


def _start_command(
    *,
    command: str,
    event_format: str | None,
    **fields: Any,
) -> _CommandContext:
    """Create one command context and emit its started lifecycle event."""
    return _common_impl.start_command(
        command=command,
        event_format=event_format,
        runtime_events=RuntimeEvents,
        emit_lifecycle_event_fn=_emit_lifecycle_event,
        perf_counter_fn=perf_counter,
        **fields,
    )


def _complete_output(
    context: _CommandContext,
    payload: Any,
    *,
    mode: _CompletionMode,
    pretty: bool = True,
    output_path: str | None = None,
    format_hint: str | None = None,
    success_message: str | None = None,
    **fields: Any,
) -> int:
    """Emit completion for *context* and route the payload by output mode."""
    return _common_impl.complete_output(
        context,
        payload,
        mode=mode,
        complete_command_fn=_complete_command,
        emit_json_payload_fn=_emit_json_payload,
        io_module=_io,
        write_file_payload_fn=_write_file_payload,
        pretty=pretty,
        output_path=output_path,
        format_hint=format_hint,
        success_message=success_message,
        **fields,
    )


def _record_run_completion(
    history_store: HistoryStore,
    context: _CommandContext,
    *,
    status: _RunCompletionStatus,
    result_summary: JSONData | None = None,
    exc: Exception | None = None,
) -> None:
    """Persist the terminal state for one tracked CLI run."""
    _common_impl.record_run_completion(
        history_store,
        context,
        status=status,
        runtime_events=RuntimeEvents,
        elapsed_ms_fn=_elapsed_ms,
        run_completion_cls=RunCompletion,
        run_state_cls=RunState,
        result_summary=result_summary,
        exc=exc,
    )


def _resolve_render_template(
    template: TemplateKey | None,
    template_path: str | None,
) -> tuple[TemplateKey | None, str | None]:
    """Resolve the render template key and optional template-file override."""
    return _common_impl.resolve_render_template(
        template,
        template_path,
        path_cls=Path,
    )


def _emit_render_output(
    rendered_chunks: list[str],
    *,
    output_path: str | None,
    pretty: bool,
    quiet: bool,
    schema_count: int,
) -> int:
    """Write rendered SQL to a file path or print it to STDOUT."""
    return _common_impl.emit_render_output(
        rendered_chunks,
        output_path=output_path,
        pretty=pretty,
        quiet=quiet,
        schema_count=schema_count,
        path_cls=Path,
    )


def _resolve_payload(
    payload: object,
    *,
    format_hint: str | None,
    format_explicit: bool,
    hydrate_files: bool = True,
) -> object:
    """Resolve one CLI payload through the shared CLI payload loader."""
    return _common_impl.resolve_payload(
        payload,
        format_hint=format_hint,
        format_explicit=format_explicit,
        hydrate_files=hydrate_files,
        io_module=_io,
    )


def _resolve_mapping_payload(
    payload: object,
    *,
    format_explicit: bool,
    error_message: str,
) -> dict[str, Any]:
    """Resolve one CLI payload and require a mapping result."""
    return _common_impl.resolve_mapping_payload(
        payload,
        format_explicit=format_explicit,
        error_message=error_message,
        resolve_payload_fn=_resolve_payload,
    )


def _emit_follow_history(
    *,
    job: str | None = None,
    limit: int | None = None,
    run_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
) -> int:
    """Stream newly observed raw history records until interrupted."""
    return _history_impl.emit_follow_history(
        load_history_records_fn=_load_history_records,
        history_view=HistoryView,
        emit_json_payload_fn=_emit_json_payload,
        sleep_fn=sleep,
        job=job,
        limit=limit,
        run_id=run_id,
        since=since,
        until=until,
        status=status,
    )


def _emit_readiness_report(
    *,
    config: str | None,
    pretty: bool,
) -> int:
    """Build and emit one readiness report, returning its CLI exit code."""
    return _common_impl.emit_readiness_report(
        config=config,
        pretty=pretty,
        readiness_builder=ReadinessReportBuilder,
        emit_json_payload_fn=_emit_json_payload,
    )


def _write_file_payload(
    payload: JSONData,
    target: str,
    *,
    format_hint: str | None,
) -> None:
    """Write a JSON-like payload to a file path using an optional format hint."""
    _common_impl.write_file_payload(
        payload,
        target,
        format_hint=format_hint,
        file_cls=File,
        file_format_cls=FileFormat,
    )


# SECTION: FUNCTIONS ======================================================== #


def check_handler(
    *,
    config: str | None = None,
    jobs: bool = False,
    pipelines: bool = False,
    readiness: bool = False,
    sources: bool = False,
    summary: bool = False,
    targets: bool = False,
    transforms: bool = False,
    substitute: bool = True,
    pretty: bool = True,
) -> int:
    """
    Print requested pipeline sections from a YAML configuration.

    Parameters
    ----------
    config : str | None, optional
        Path to the pipeline YAML configuration.
    jobs : bool, optional
        Whether to include job metadata. Default is ``False``.
    pipelines : bool, optional
        Whether to include pipeline metadata. Default is ``False``.
    readiness : bool, optional
        Whether to run runtime and config readiness checks. Default is
        ``False``.
    sources : bool, optional
        Whether to include source metadata. Default is ``False``.
    summary : bool, optional
        Whether to print a full summary of the pipeline. Default is ``False``.
    targets : bool, optional
        Whether to include target metadata. Default is ``False``.
    transforms : bool, optional
        Whether to include transform metadata. Default is ``False``.
    substitute : bool, optional
        Whether to perform environment variable substitution. Default is
        ``True``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.

    Returns
    -------
    int
        Zero on success.

    Raises
    ------
    ValueError
        If config inspection is requested without a configuration path.

    """
    return _check_impl.check_handler(
        config=config,
        jobs=jobs,
        pipelines=pipelines,
        readiness=readiness,
        sources=sources,
        summary=summary,
        targets=targets,
        transforms=transforms,
        substitute=substitute,
        pretty=pretty,
        config_cls=Config,
        emit_json_payload_fn=_emit_json_payload,
        emit_readiness_report_fn=_emit_readiness_report,
        check_sections_fn=_check_sections,
        pipeline_summary_fn=_pipeline_summary,
    )


def history_handler(
    *,
    follow: bool = False,
    job: str | None = None,
    json_output: bool = False,
    limit: int | None = None,
    raw: bool = False,
    pretty: bool = True,
    run_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
    table: bool = False,
) -> int:
    """
    Emit persisted local run history.

    Parameters
    ----------
    follow : bool, optional
        Whether to keep polling for new matching raw history records.
        Default is ``False``.
    job : str | None, optional
        Restrict records to the given job name. Default is ``None``.
    json_output : bool, optional
        Whether to emit JSON explicitly. Default is ``False``.
    limit : int | None, optional
        Maximum number of records to emit. Default is ``None``.
    raw : bool, optional
        Whether to emit raw append events instead of normalized runs.
        Default is ``False``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.
    run_id : str | None, optional
        Restrict records to the given run identifier. Default is ``None``.
    since : str | None, optional
        Restrict records to runs at or after the given ISO-8601 timestamp.
        Default is ``None``.
    until : str | None, optional
        Restrict records to runs at or before the given ISO-8601 timestamp.
        Default is ``None``.
    status : str | None, optional
        Restrict records to the given persisted status. Default is ``None``.
    table : bool, optional
        Whether to emit the filtered result set as a Markdown table instead of
        JSON. Default is ``False``.

    Returns
    -------
    int
        Zero on success.
    """
    return _history_impl.history_handler(
        follow=follow,
        job=job,
        json_output=json_output,
        limit=limit,
        raw=raw,
        pretty=pretty,
        run_id=run_id,
        since=since,
        until=until,
        status=status,
        table=table,
        emit_follow_history_fn=_emit_follow_history,
        emit_history_payload_fn=_emit_history_payload,
        load_history_records_fn=_load_history_records,
        columns=_HISTORY_TABLE_COLUMNS,
    )


def extract_handler(
    *,
    source_type: str,
    source: str,
    event_format: str | None = None,
    format_hint: str | None = None,
    format_explicit: bool = False,
    target: str | None = None,
    output: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Extract data from a source.

    Parameters
    ----------
    source_type : str
        The type of the source (e.g., 'file', 'api', 'database').
    source : str
        The source identifier (e.g., path, URL, DSN).
    event_format : str | None, optional
        Optional structured event format emitted to STDERR. Default is
        ``None``.
    format_hint : str | None, optional
        An optional format hint (e.g., 'json', 'csv'). Default is ``None``.
    format_explicit : bool, optional
        Whether the format hint was explicitly provided. Default is ``False``.
    target : str | None, optional
        The target destination (e.g., path, database). Default is ``None``.
    output : str | None, optional
        Path to write output data. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.

    Returns
    -------
    int
        Zero on success.
    """
    return _dataops_impl.extract_handler(
        source_type=source_type,
        source=source,
        event_format=event_format,
        format_hint=format_hint,
        format_explicit=format_explicit,
        target=target,
        output=output,
        pretty=pretty,
        extract_fn=extract,
        io_module=_io,
        start_command_fn=_start_command,
        failure_boundary_fn=_failure_boundary,
        complete_output_fn=_complete_output,
    )


def load_handler(
    *,
    source: str,
    target_type: str,
    target: str,
    event_format: str | None = None,
    source_format: str | None = None,
    target_format: str | None = None,
    format_explicit: bool = False,
    output: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Load data into a target.

    Parameters
    ----------
    source : str
        The source payload (e.g., path, inline data).
    target_type : str
        The type of the target (e.g., 'file', 'database').
    target : str
        The target destination (e.g., path, DSN).
    event_format : str | None, optional
        Optional structured event format emitted to STDERR. Default is
        ``None``.
    source_format : str | None, optional
        An optional source format hint (e.g., 'json', 'csv'). Default is
        ``None``.
    target_format : str | None, optional
        An optional target format hint (e.g., 'json', 'csv'). Default is
        ``None``.
    format_explicit : bool, optional
        Whether the format hint was explicitly provided. Default is ``False``.
    output : str | None, optional
        Path to write output data. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.

    Returns
    -------
    int
        Zero on success.
    """
    return _dataops_impl.load_handler(
        source=source,
        target_type=target_type,
        target=target,
        event_format=event_format,
        source_format=source_format,
        target_format=target_format,
        format_explicit=format_explicit,
        output=output,
        pretty=pretty,
        load_fn=load,
        io_module=_io,
        start_command_fn=_start_command,
        failure_boundary_fn=_failure_boundary,
        resolve_payload_fn=_resolve_payload,
        complete_output_fn=_complete_output,
    )


def render_handler(
    *,
    config: str | None = None,
    spec: str | None = None,
    table: str | None = None,
    template: TemplateKey | None = None,
    template_path: str | None = None,
    output: str | None = None,
    pretty: bool = True,
    quiet: bool = False,
) -> int:
    """
    Render SQL DDL statements from table schema specs.

    Parameters
    ----------
    config : str | None, optional
        Path to a pipeline YAML configuration. Default is ``None``.
    spec : str | None, optional
        Path to a standalone table spec file. Default is ``None``.
    table : str | None, optional
        Table name filter. Default is ``None``.
    template : TemplateKey | None, optional
        The template key to use for rendering. Default is ``None``.
    template_path : str | None, optional
        Path to a custom template file. Default is ``None``.
    output : str | None, optional
        Path to write output SQL. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.
    quiet : bool, optional
        Whether to suppress non-error output. Default is ``False``.

    Returns
    -------
    int
        Zero on success.
    """
    return _render_impl.render_handler(
        config=config,
        spec=spec,
        table=table,
        template=template,
        template_path=template_path,
        output=output,
        pretty=pretty,
        quiet=quiet,
        resolve_render_template_fn=_resolve_render_template,
        summary_module=_summary,
        render_tables_fn=render_tables,
        emit_render_output_fn=_emit_render_output,
        print_fn=print,
        stderr=sys.stderr,
    )


def report_handler(
    *,
    group_by: Literal['day', 'job', 'status'] = 'job',
    job: str | None = None,
    json_output: bool = False,
    pretty: bool = True,
    since: str | None = None,
    table: bool = False,
    until: str | None = None,
) -> int:
    """
    Emit a grouped history report derived from normalized persisted runs.

    Parameters
    ----------
    group_by : Literal['day', 'job', 'status'], optional
        Field on which to group the report rows. Default is ``'job'``.
    job : str | None, optional
        Restrict source records to the given job name. Default is ``None``.
    json_output : bool, optional
        Whether to emit JSON explicitly. Default is ``False``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.
    since : str | None, optional
        Restrict records to runs at or after the given timestamp.
        Default is ``None``.
    table : bool, optional
        Whether to emit grouped rows as a Markdown table. Default is ``False``.
    until : str | None, optional
        Restrict records to runs at or before the given timestamp.
        Default is ``None``.

    Returns
    -------
    int
        Zero on success.
    """
    return _history_impl.report_handler(
        group_by=group_by,
        job=job,
        json_output=json_output,
        pretty=pretty,
        since=since,
        table=table,
        until=until,
        load_history_records_fn=_load_history_records,
        report_builder=HistoryReportBuilder,
        emit_history_payload_fn=_emit_history_payload,
        columns=_REPORT_TABLE_COLUMNS,
    )


def run_handler(
    *,
    config: str,
    job: str | None = None,
    pipeline: str | None = None,
    event_format: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Execute an ETL job end-to-end from a pipeline YAML configuration.

    Parameters
    ----------
    config : str
        Path to the pipeline YAML configuration.
    job : str | None, optional
        Name of the job to run. If not provided, runs the entire pipeline.
        Default is ``None``.
    pipeline : str | None, optional
        Alias for *job*. Default is ``None``.
    event_format : str | None, optional
        Optional structured event format emitted to STDERR. Default is
        ``None``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.

    Returns
    -------
    int
        Zero on success.
    """
    cfg = Config.from_yaml(config, substitute=True)

    job_name = job or pipeline
    if not job_name:
        return _emit_json_payload(_pipeline_summary(cfg), pretty=pretty)

    context = _start_command(
        command='run',
        event_format=event_format,
        config_path=config,
        etlplus_version=__version__,
        job=job_name,
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
            job_name=job_name,
        ),
    )

    with _failure_boundary(
        context,
        on_error=lambda exc: _record_run_completion(
            history_store,
            context,
            status='failed',
            exc=exc,
        ),
        config_path=config,
        job=job_name,
        pipeline_name=cfg.name,
    ):
        result = run(job=job_name, config_path=config)

    _record_run_completion(
        history_store,
        context,
        status='succeeded',
        result_summary=cast(JSONData | None, result),
    )
    return _complete_output(
        context,
        {'run_id': context.run_id, 'status': 'ok', 'result': result},
        mode='json',
        pretty=pretty,
        config_path=config,
        job=job_name,
        pipeline_name=cfg.name,
        result_status=result.get('status'),
        status='ok',
    )


def status_handler(
    *,
    job: str | None = None,
    pretty: bool = True,
    run_id: str | None = None,
) -> int:
    """
    Emit the latest normalized run matching the given status filters.

    Parameters
    ----------
    job : str | None, optional
        Restrict the lookup to the given job name. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.
    run_id : str | None, optional
        Restrict the lookup to the given run identifier. Default is ``None``.

    Returns
    -------
    int
        Zero when a matching run exists, otherwise ``1``.
    """
    return _history_impl.status_handler(
        job=job,
        pretty=pretty,
        run_id=run_id,
        load_history_records_fn=_load_history_records,
        emit_json_payload_fn=_emit_json_payload,
    )


def transform_handler(
    *,
    source: str,
    operations: JSONData | str,
    target: str | None = None,
    target_type: str | None = None,
    event_format: str | None = None,
    source_format: str | None = None,
    target_format: str | None = None,
    pretty: bool = True,
    format_explicit: bool = False,
) -> int:
    """
    Transform data from a source.

    Parameters
    ----------
    source : str
        The source payload (e.g., path, inline data).
    operations : JSONData | str
        The transformation operations (inline JSON or path).
    target : str | None, optional
        The target destination (e.g., file path, URI, or connector target).
        Default is ``None``.
    target_type : str | None, optional
        The target connector type (e.g., ``'file'``, ``'api'``,
        ``'database'``). Default is ``None``.
    event_format : str | None, optional
        Optional structured event format emitted to STDERR. Default is
        ``None``.
    source_format : str | None, optional
        An optional source format hint (e.g., 'json', 'csv'). Default is
        ``None``.
    target_format : str | None, optional
        An optional target format hint (e.g., 'json', 'csv'). Default is
        ``None``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.
    format_explicit : bool, optional
        Whether the format hint was explicitly provided. Default is ``False``.

    Returns
    -------
    int
        Zero on success.

    Notes
    -----
    File targets are written directly. Non-file targets such as ``api`` and
    ``database`` are delegated to :func:`etlplus.ops.load.load` so the
    transform command and load command share target behavior.
    """
    return _dataops_impl.transform_handler(
        source=source,
        operations=operations,
        target=target,
        target_type=target_type,
        event_format=event_format,
        source_format=source_format,
        target_format=target_format,
        pretty=pretty,
        format_explicit=format_explicit,
        load_fn=load,
        transform_fn=transform,
        start_command_fn=_start_command,
        failure_boundary_fn=_failure_boundary,
        resolve_payload_fn=_resolve_payload,
        resolve_mapping_payload_fn=_resolve_mapping_payload,
        complete_output_fn=_complete_output,
    )


def validate_handler(
    *,
    source: str,
    rules: JSONData | str,
    event_format: str | None = None,
    source_format: str | None = None,
    target: str | None = None,
    format_explicit: bool = False,
    pretty: bool = True,
) -> int:
    """
    Validate data from a source.

    Parameters
    ----------
    source : str
        The source payload (e.g., path, inline data).
    rules : JSONData | str
        The validation rules (inline JSON or path).
    event_format : str | None, optional
        Optional structured event format emitted to STDERR. Default is
        ``None``.
    source_format : str | None, optional
        An optional source format hint (e.g., 'json', 'csv'). Default is
        ``None``.
    target : str | None, optional
        The target destination (e.g., path). Default is ``None``.
    format_explicit : bool, optional
        Whether the format hint was explicitly provided. Default is ``False``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.

    Returns
    -------
    int
        Zero on success.
    """
    return _dataops_impl.validate_handler(
        source=source,
        rules=rules,
        event_format=event_format,
        source_format=source_format,
        target=target,
        format_explicit=format_explicit,
        pretty=pretty,
        validate_fn=validate,
        start_command_fn=_start_command,
        failure_boundary_fn=_failure_boundary,
        resolve_payload_fn=_resolve_payload,
        resolve_mapping_payload_fn=_resolve_mapping_payload,
        complete_output_fn=_complete_output,
        print_fn=print,
        stderr=sys.stderr,
    )
