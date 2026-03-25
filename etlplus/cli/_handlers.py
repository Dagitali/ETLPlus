"""
:mod:`etlplus.cli.handlers` module.

Command handler functions for the ``etlplus`` command-line interface (CLI).
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from time import sleep
from typing import Any
from typing import Literal
from typing import cast

from .. import Config
from .. import __version__
from ..database import load_table_spec
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
from ..ops.validate import FieldRulesDict
from ..runtime import ReadinessReportBuilder
from ..runtime.events import RuntimeEvents
from ..utils.types import JSONData
from ..utils.types import TemplateKey
from . import _io
from ._history import HISTORY_TABLE_COLUMNS as _HISTORY_TABLE_COLUMNS
from ._history import REPORT_TABLE_COLUMNS as _REPORT_TABLE_COLUMNS
from ._history import HistoryReportBuilder
from ._history import HistoryView

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


# SECTION: TYPE ALIASES ===================================================== #


type TransformOperations = Mapping[
    Literal['filter', 'map', 'select', 'sort', 'aggregate'],
    Any,
]


# SECTION: INTERNAL DATA CLASSES ============================================ #


@dataclass(frozen=True, slots=True)
class _CommandContext:
    """Shared runtime context for one CLI command invocation."""

    # -- Instance Attributes -- #

    command: str
    event_format: str | None
    run_id: str
    started_at: str
    started_perf: float


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _collect_table_specs(
    config_path: str | None,
    spec_path: str | None,
) -> list[dict[str, Any]]:
    """
    Load table schemas from a pipeline config and/or standalone spec.

    Parameters
    ----------
    config_path : str | None
        Path to a pipeline YAML config file.
    spec_path : str | None
        Path to a standalone table spec file.

    Returns
    -------
    list[dict[str, Any]]
        Collected table specification mappings.
    """
    specs: list[dict[str, Any]] = []

    if spec_path:
        specs.append(dict(load_table_spec(Path(spec_path))))

    if config_path:
        cfg = Config.from_yaml(config_path, substitute=True)
        specs.extend(getattr(cfg, 'table_schemas', []))

    return specs


def _check_sections(
    cfg: Config,
    *,
    jobs: bool,
    pipelines: bool,
    sources: bool,
    targets: bool,
    transforms: bool,
) -> dict[str, Any]:
    """
    Build sectioned metadata output for the check command.

    Parameters
    ----------
    cfg : Config
        The loaded pipeline configuration.
    jobs : bool
        Whether to include job metadata.
    pipelines : bool
        Whether to include pipeline metadata.
    sources : bool
        Whether to include source metadata.
    targets : bool
        Whether to include target metadata.
    transforms : bool
        Whether to include transform metadata.

    Returns
    -------
    dict[str, Any]
        Metadata output for the check command.
    """
    sections: dict[str, Any] = {}
    if jobs:
        sections['jobs'] = _pipeline_summary(cfg)['jobs']
    if pipelines:
        sections['pipelines'] = [cfg.name]
    if sources:
        sections['sources'] = [src.name for src in cfg.sources]
    if targets:
        sections['targets'] = [tgt.name for tgt in cfg.targets]
    if transforms:
        if isinstance(cfg.transforms, Mapping):
            sections['transforms'] = list(cfg.transforms)
        else:
            sections['transforms'] = [
                getattr(trf, 'name', None) for trf in cfg.transforms
            ]
    if not sections:
        sections['jobs'] = _pipeline_summary(cfg)['jobs']
    return sections


def _elapsed_ms(
    started_perf: float,
) -> int:
    """Return elapsed milliseconds since *started_perf*."""
    return int((perf_counter() - started_perf) * 1000)


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
    _emit_lifecycle_event(
        command=command,
        lifecycle='failed',
        run_id=run_id,
        event_format=event_format,
        duration_ms=int((perf_counter() - started_perf) * 1000),
        error_message=str(exc),
        error_type=type(exc).__name__,
        status='error',
        **fields,
    )


def _emit_json_payload(
    payload: Any,
    *,
    pretty: bool,
    exit_code: int = 0,
) -> int:
    """Emit one JSON payload and return the requested exit code."""
    _io.emit_json(payload, pretty=pretty)
    return exit_code


def _emit_lifecycle_event(
    *,
    command: str,
    lifecycle: str,
    run_id: str,
    event_format: str | None,
    **fields: Any,
) -> None:
    """Emit one structured command lifecycle event."""
    RuntimeEvents.emit(
        RuntimeEvents.build(
            command=command,
            lifecycle=lifecycle,
            run_id=run_id,
            **fields,
        ),
        event_format=event_format,
    )


def _emit_or_write_payload(
    payload: Any,
    output_path: str | None,
    *,
    pretty: bool,
    success_message: str,
) -> int:
    """Emit one payload to stdout or write it to the requested output path."""
    _io.emit_or_write(
        payload,
        output_path,
        pretty=pretty,
        success_message=success_message,
    )
    return 0


def _emit_table_payload(
    rows: list[dict[str, Any]],
    *,
    columns: tuple[str, ...],
    exit_code: int = 0,
) -> int:
    """Emit one Markdown table payload and return the requested exit code."""
    _io.emit_markdown_table(rows, columns=columns)
    return exit_code


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


def _load_history_records(
    *,
    raw: bool,
    job: str | None = None,
    limit: int | None = None,
    run_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    """Load, filter, and sort history records for CLI read commands."""
    return HistoryView.load_records(
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
    context = _CommandContext(
        command=command,
        event_format=event_format,
        run_id=RuntimeEvents.create_run_id(),
        started_at=RuntimeEvents.utc_now_iso(),
        started_perf=perf_counter(),
    )
    _emit_lifecycle_event(
        command=context.command,
        lifecycle='started',
        run_id=context.run_id,
        event_format=context.event_format,
        timestamp=context.started_at,
        **fields,
    )
    return context


def _complete_and_emit_json(
    context: _CommandContext,
    payload: Any,
    *,
    pretty: bool,
    **fields: Any,
) -> int:
    """Emit the completion event for *context* and then emit one JSON payload."""
    _complete_command(context, **fields)
    return _emit_json_payload(payload, pretty=pretty)


def _complete_and_emit_or_write(
    context: _CommandContext,
    payload: Any,
    output_path: str | None,
    *,
    pretty: bool,
    success_message: str,
    **fields: Any,
) -> int:
    """Emit completion for *context* and route payload to stdout or file."""
    _complete_command(context, **fields)
    return _emit_or_write_payload(
        payload,
        output_path,
        pretty=pretty,
        success_message=success_message,
    )


def _complete_and_write_file_payload(
    context: _CommandContext,
    payload: JSONData,
    output_target: str,
    *,
    format_hint: str | None,
    success_message: str,
    **fields: Any,
) -> int:
    """Emit completion for *context*, write one file payload, and confirm it."""
    _complete_command(context, **fields)
    _write_file_payload(payload, output_target, format_hint=format_hint)
    print(f'{success_message} {output_target}')
    return 0


def _complete_and_write_json_output(
    context: _CommandContext,
    payload: Any,
    output_target: str,
    *,
    success_message: str,
    **fields: Any,
) -> int:
    """Emit completion for *context* and write one JSON payload to *target*."""
    _complete_command(context, **fields)
    _io.write_json_output(
        payload,
        output_target,
        success_message=success_message,
    )
    return 0


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
    seen: set[str] = set()
    try:
        while True:
            records = _load_history_records(
                job=job,
                limit=limit,
                raw=True,
                run_id=run_id,
                since=since,
                until=until,
                status=status,
            )
            for record in reversed(records):
                fingerprint = HistoryView.fingerprint(record)
                if fingerprint in seen:
                    continue
                seen.add(fingerprint)
                _emit_json_payload(record, pretty=False)
            sleep(1.0)
    except KeyboardInterrupt:
        return 0


def _emit_readiness_report(
    *,
    config: str | None,
    pretty: bool,
) -> int:
    """Build and emit one readiness report, returning its CLI exit code."""
    report = ReadinessReportBuilder.build(config_path=config)
    return _emit_json_payload(
        report,
        pretty=pretty,
        exit_code=0 if report.get('status') == 'ok' else 1,
    )


def _emit_json_or_table(
    payload: Any,
    *,
    columns: tuple[str, ...],
    pretty: bool,
    table: bool,
    table_rows: list[dict[str, Any]] | None = None,
    exit_code: int = 0,
) -> int:
    """Emit one payload as JSON or as a Markdown table and return an exit code."""
    if table:
        return _emit_table_payload(
            table_rows
            if table_rows is not None
            else cast(list[dict[str, Any]], payload),
            columns=columns,
            exit_code=exit_code,
        )
    return _emit_json_payload(payload, pretty=pretty, exit_code=exit_code)


def _pipeline_summary(
    cfg: Config,
) -> dict[str, Any]:
    """
    Return a human-friendly snapshot of a pipeline config.

    Parameters
    ----------
    cfg : Config
        The loaded pipeline configuration.

    Returns
    -------
    dict[str, Any]
        A human-friendly snapshot of a pipeline config.
    """
    sources = [src.name for src in cfg.sources]
    targets = [tgt.name for tgt in cfg.targets]
    jobs = [job.name for job in cfg.jobs]
    return {
        'name': cfg.name,
        'version': cfg.version,
        'sources': sources,
        'targets': targets,
        'jobs': jobs,
    }


def _write_file_payload(
    payload: JSONData,
    target: str,
    *,
    format_hint: str | None,
) -> None:
    """
    Write a JSON-like payload to a file path using an optional format hint.

    Parameters
    ----------
    payload : JSONData
        The structured data to write.
    target : str
        File path to write to.
    format_hint : str | None
        Optional format hint for :class:`FileFormat`.
    """
    file_format = FileFormat.coerce(format_hint) if format_hint else None
    File(target, file_format=file_format).write(payload)


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
    if readiness:
        return _emit_readiness_report(config=config, pretty=pretty)

    if config is None:
        raise ValueError('config is required unless readiness-only mode is used')

    cfg = Config.from_yaml(config, substitute=substitute)
    if summary:
        return _emit_json_payload(_pipeline_summary(cfg), pretty=True)

    return _emit_json_payload(
        _check_sections(
            cfg,
            jobs=jobs,
            pipelines=pipelines,
            sources=sources,
            targets=targets,
            transforms=transforms,
        ),
        pretty=pretty,
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
    HistoryView.validate_output_mode(json_output=json_output, table=table)
    if follow:
        return _emit_follow_history(
            job=job,
            limit=limit,
            run_id=run_id,
            since=since,
            status=status,
            until=until,
        )
    records = _load_history_records(
        job=job,
        limit=limit,
        raw=raw,
        run_id=run_id,
        since=since,
        until=until,
        status=status,
    )
    return _emit_json_or_table(
        records,
        columns=_HISTORY_TABLE_COLUMNS,
        pretty=pretty,
        table=table,
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

    Raises
    ------
    Exception
        Re-raises extraction failures after emitting a structured failure event
        when requested.

    """
    explicit_format = format_hint if format_explicit else None
    context = _start_command(
        command='extract',
        event_format=event_format,
        source=source,
        source_type=source_type,
    )

    try:
        if source == '-':
            text = _io.read_stdin_text()
            payload = _io.parse_text_payload(
                text,
                format_hint,
            )
            return _complete_and_emit_json(
                context,
                payload,
                pretty=pretty,
                result_status='ok',
                status='ok',
                source=source,
                source_type=source_type,
            )

        result = extract(
            source_type,
            source,
            file_format=explicit_format,
        )
        output_path = target or output

        return _complete_and_emit_or_write(
            context,
            result,
            output_path,
            pretty=pretty,
            success_message='Data extracted and saved to',
            destination=output_path or 'stdout',
            result_status='ok',
            source=source,
            source_type=source_type,
            status='ok',
        )
    except Exception as exc:
        _fail_command(
            context,
            exc,
            source=source,
            source_type=source_type,
        )
        raise


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

    Raises
    ------
    Exception
        Re-raises load failures after emitting a structured failure event when
        requested.
    """
    explicit_format = target_format if format_explicit else None
    context = _start_command(
        command='load',
        event_format=event_format,
        source=source,
        target=target,
        target_type=target_type,
    )

    try:
        # Allow piping into load.
        source_value = cast(
            str | Path | os.PathLike[str] | dict[str, Any] | list[dict[str, Any]],
            _io.resolve_cli_payload(
                source,
                format_hint=source_format,
                format_explicit=source_format is not None,
                hydrate_files=False,
            ),
        )

        # Allow piping out of load for file targets.
        if target_type == 'file' and target == '-':
            payload = _io.materialize_file_payload(
                source_value,
                format_hint=source_format,
                format_explicit=source_format is not None,
            )
            return _complete_and_emit_json(
                context,
                payload,
                pretty=pretty,
                result_status='ok',
                source=source,
                status='ok',
                target=target,
                target_type=target_type,
            )

        result = load(
            source_value,
            target_type,
            target,
            file_format=explicit_format,
        )

        output_path = output
        return _complete_and_emit_or_write(
            context,
            result,
            output_path,
            pretty=pretty,
            success_message='Load result saved to',
            destination=output_path or 'stdout',
            result_status=result.get('status') if isinstance(result, dict) else 'ok',
            source=source,
            status='ok',
            target=target,
            target_type=target_type,
        )
    except Exception as exc:
        _fail_command(
            context,
            exc,
            source=source,
            target=target,
            target_type=target_type,
        )
        raise


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
    template_value: TemplateKey = template or 'ddl'
    template_path_override = template_path
    table_filter = table
    spec_path = spec
    config_path = config

    # If the provided template points to a file, treat it as a path override.
    file_override = template_path_override
    template_key: TemplateKey | None = template_value
    if template_path_override is None:
        candidate_path = Path(template_value)
        if candidate_path.exists():
            file_override = str(candidate_path)
            template_key = None

    specs = _collect_table_specs(config_path, spec_path)
    if table_filter:
        specs = [
            spec
            for spec in specs
            if str(spec.get('table')) == table_filter
            or str(spec.get('name', '')) == table_filter
        ]

    if not specs:
        target_desc = table_filter or 'table_schemas'
        print(
            'No table schemas found for '
            f'{target_desc}. Provide --spec or a pipeline --config with '
            'table_schemas.',
            file=sys.stderr,
        )
        return 1

    rendered_chunks = render_tables(
        specs,
        template=template_key,
        template_path=file_override,
    )
    sql_text = '\n'.join(chunk.rstrip() for chunk in rendered_chunks).rstrip() + '\n'
    rendered_output = sql_text if pretty else sql_text.rstrip('\n')

    output_path = output
    if output_path and output_path != '-':
        Path(output_path).write_text(rendered_output, encoding='utf-8')
        if not quiet:
            print(f'Rendered {len(specs)} schema(s) to {output_path}')
        return 0

    print(rendered_output)
    return 0


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
    HistoryView.validate_output_mode(json_output=json_output, table=table)
    records = HistoryView.load_records(
        job=job,
        raw=False,
        since=since,
        until=until,
    )
    report = HistoryReportBuilder.build(records, group_by=group_by)
    return _emit_json_or_table(
        report,
        columns=_REPORT_TABLE_COLUMNS,
        pretty=pretty,
        table=table,
        table_rows=cast(list[dict[str, Any]], report['rows']),
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

    Raises
    ------
    Exception
        Re-raises the underlying execution error after emitting a failure
        event when structured event output is enabled.
    """
    cfg = Config.from_yaml(config, substitute=True)

    job_name = job or pipeline
    if job_name:
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
        try:
            result = run(job=job_name, config_path=config)
        except Exception as exc:
            duration_ms = _elapsed_ms(context.started_perf)
            history_store.record_run_finished(
                RunCompletion(
                    run_id=context.run_id,
                    state=RunState(
                        status='failed',
                        finished_at=RuntimeEvents.utc_now_iso(),
                        duration_ms=duration_ms,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    ),
                ),
            )
            _fail_command(
                context,
                exc,
                config_path=config,
                job=job_name,
                pipeline_name=cfg.name,
            )
            raise

        duration_ms = _elapsed_ms(context.started_perf)
        history_store.record_run_finished(
            RunCompletion(
                run_id=context.run_id,
                state=RunState(
                    status='succeeded',
                    finished_at=RuntimeEvents.utc_now_iso(),
                    duration_ms=duration_ms,
                    result_summary=cast(JSONData | None, result),
                ),
            ),
        )
        _complete_command(
            context,
            config_path=config,
            job=job_name,
            pipeline_name=cfg.name,
            result_status=result.get('status'),
            status='ok',
        )
        return _emit_json_payload(
            {'run_id': context.run_id, 'status': 'ok', 'result': result},
            pretty=pretty,
        )

    return _emit_json_payload(_pipeline_summary(cfg), pretty=pretty)


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
    records = HistoryView.load_records(
        job=job,
        limit=1,
        raw=False,
        run_id=run_id,
    )
    if not records:
        return _emit_json_payload({}, pretty=pretty, exit_code=1)
    return _emit_json_payload(records[0], pretty=pretty)


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

    Raises
    ------
    ValueError
        If the operations payload is not a mapping.
    Exception
        Re-raises transform failures after emitting a structured failure event
        when requested.

    Notes
    -----
    File targets are written directly. Non-file targets such as ``api`` and
    ``database`` are delegated to :func:`etlplus.ops.load.load` so the
    transform command and load command share target behavior.
    """
    format_hint: str | None = source_format
    format_explicit = format_hint is not None or format_explicit
    context = _start_command(
        command='transform',
        event_format=event_format,
        source=source,
        target=target or 'stdout',
        target_type=target_type,
    )

    try:
        payload = cast(
            JSONData | str,
            _io.resolve_cli_payload(
                source,
                format_hint=format_hint,
                format_explicit=format_explicit,
            ),
        )

        operations_payload = _io.resolve_cli_payload(
            operations,
            format_hint=None,
            format_explicit=format_explicit,
        )
        if not isinstance(operations_payload, dict):
            raise ValueError('operations must resolve to a mapping of transforms')

        data = transform(payload, cast(TransformOperations, operations_payload))

        if target and target != '-':
            if target_type not in (None, 'file'):
                resolved_target_type = cast(str, target_type)
                result = load(
                    data,
                    resolved_target_type,
                    target,
                    file_format=target_format if format_explicit else None,
                )
                return _complete_and_emit_json(
                    context,
                    result,
                    pretty=pretty,
                    result_status='ok',
                    source=source,
                    status='ok',
                    target=target,
                    target_type=resolved_target_type,
                )

            return _complete_and_write_file_payload(
                context,
                data,
                target,
                format_hint=target_format,
                success_message='Data transformed and saved to',
                result_status='ok',
                source=source,
                status='ok',
                target=target,
                target_type=target_type or 'file',
            )

        return _complete_and_emit_json(
            context,
            data,
            pretty=pretty,
            result_status='ok',
            source=source,
            status='ok',
            target=target or 'stdout',
            target_type=target_type,
        )
    except Exception as exc:
        _fail_command(
            context,
            exc,
            source=source,
            target=target or 'stdout',
            target_type=target_type,
        )
        raise


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

    Raises
    ------
    ValueError
        If the rules payload is not a mapping.
    Exception
        Re-raises validation failures after emitting a structured failure event
        when requested.
    """
    format_hint: str | None = source_format
    context = _start_command(
        command='validate',
        event_format=event_format,
        source=source,
        target=target or 'stdout',
    )

    try:
        payload = cast(
            JSONData | str,
            _io.resolve_cli_payload(
                source,
                format_hint=format_hint,
                format_explicit=format_explicit,
            ),
        )

        rules_payload = _io.resolve_cli_payload(
            rules,
            format_hint=None,
            format_explicit=format_explicit,
        )
        if not isinstance(rules_payload, dict):
            raise ValueError('rules must resolve to a mapping of field rules')

        field_rules = cast(Mapping[str, FieldRulesDict], rules_payload)
        result = validate(payload, field_rules)

        if target and target != '-':
            validated_data = result.get('data')
            if validated_data is not None:
                return _complete_and_write_json_output(
                    context,
                    validated_data,
                    target,
                    success_message='ValidationDict result saved to',
                    result_status='ok',
                    source=source,
                    status='ok',
                    target=target,
                    valid=result.get('valid'),
                )
            print(
                f'ValidationDict failed, no data to save for {target}',
                file=sys.stderr,
            )
            return 0

        return _complete_and_emit_json(
            context,
            result,
            pretty=pretty,
            result_status='ok',
            source=source,
            status='ok',
            target=target or 'stdout',
            valid=result.get('valid'),
        )
    except Exception as exc:
        _fail_command(
            context,
            exc,
            source=source,
            target=target or 'stdout',
        )
        raise
