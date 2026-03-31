"""
:mod:`etlplus.cli._handler_common` module.

Shared implementation helpers for the :mod:`etlplus.cli._handlers` facade.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any
from typing import cast

from ..file import File
from ..file import FileFormat
from ..history import RunCompletion
from ..history import RunState
from ..runtime import ReadinessReportBuilder
from ..runtime import RuntimeEvents
from ..utils._types import JSONData
from ..utils._types import TemplateKey
from . import _io
from ._history import HistoryView

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'CommandContext',
    # Funnctions
    'complete_command',
    'complete_output',
    'elapsed_ms',
    'emit_failure_event',
    'emit_history_payload',
    'emit_json_payload',
    'emit_lifecycle_event',
    'emit_readiness_report',
    'emit_render_output',
    'fail_command',
    'record_run_completion',
    'resolve_mapping_payload',
    'resolve_payload',
    'resolve_render_template',
    'start_command',
    'write_file_payload',
]


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class CommandContext:
    """Runtime context for one CLI command invocation."""

    # -- Instance Attributes -- #

    command: str
    event_format: str | None
    run_id: str
    started_at: str
    started_perf: float


# SECTION: FUNCTIONS ======================================================== #


def elapsed_ms(
    started_perf: float,
    *,
    perf_counter_fn: Any = perf_counter,
) -> int:
    """
    Return elapsed milliseconds since *started_perf*.

    Parameters
    ----------
    started_perf : float
        The starting performance counter value to compare against.
    perf_counter_fn : Any
        The performance counter function to use for measuring elapsed time.
        Defaults to :func:`time.perf_counter` but can be overridden for
        testing.

    Returns
    -------
    int
        Elapsed milliseconds since *started_perf*.
    """
    return int((perf_counter_fn() - started_perf) * 1000)


def emit_lifecycle_event(
    *,
    command: str,
    lifecycle: str,
    run_id: str,
    event_format: str | None,
    **fields: Any,
) -> None:
    """
    Emit one structured command lifecycle event.

    Parameters
    ----------
    command : str
        The CLI command name, e.g. "run" or "render".
    lifecycle : str
        The lifecycle stage, e.g. "started", "completed", or "failed".
    run_id : str
        The unique identifier for this command run.
    event_format : str | None
        The requested event output format, e.g. "jsonl" or None for no events.

        emission.
    **fields : Any
        Additional fields to include in the emitted event payload.
    """
    RuntimeEvents.emit(
        RuntimeEvents.build(
            command=command,
            lifecycle=lifecycle,
            run_id=run_id,
            **fields,
        ),
        event_format=event_format,
    )


def emit_failure_event(
    *,
    command: str,
    run_id: str,
    started_perf: float,
    event_format: str | None,
    exc: Exception,
    **fields: Any,
) -> None:
    """
    Emit a failed command lifecycle event with the shared stable schema.

    Parameters
    ----------
    command : str
        The CLI command name, e.g. "run" or "render".
    run_id : str
        The unique identifier for this command run.
    started_perf : float
        The starting performance counter value to compare against.
    event_format : str | None
        The requested event output format, e.g. "jsonl" or None for no events.
    exc : Exception
        The exception that caused the failure.
    **fields : Any
        Additional fields to include in the emitted event payload.
    """
    emit_lifecycle_event(
        command=command,
        lifecycle='failed',
        run_id=run_id,
        event_format=event_format,
        duration_ms=elapsed_ms(started_perf),
        error_message=str(exc),
        error_type=type(exc).__name__,
        status='error',
        **fields,
    )


def emit_json_payload(
    payload: Any,
    *,
    pretty: bool,
    io_module: Any = _io,
    exit_code: int = 0,
) -> int:
    """
    Emit one JSON payload and return *exit_code*.

    Parameters
    ----------
    payload : Any
        The JSON-serializable payload to emit.
    pretty : bool
        Whether to pretty-print the JSON output.
    io_module : Any
        The I/O module to use for emitting JSON.
    exit_code : int, optional
        The exit code to return after emitting the JSON payload (default is 0).

    Returns
    -------
    int
        The exit code to return after emitting the JSON payload.
    """
    io_module.emit_json(payload, pretty=pretty)
    return exit_code


def emit_history_payload(
    payload: Any,
    *,
    columns: tuple[str, ...],
    pretty: bool,
    table: bool = False,
    json_output: bool = False,
    table_rows: list[dict[str, Any]] | None = None,
    exit_code: int = 0,
) -> int:
    """
    Emit history data as JSON or a Markdown table.

    Parameters
    ----------
    payload : Any
        The JSON-serializable payload to emit.
    columns : tuple[str, ...]
        The columns to include in the table output.
    pretty : bool
        Whether to pretty-print the JSON output.
    table : bool, optional
        Whether to emit the data as a Markdown table. Default is ``False``.
    json_output : bool, optional
        Whether to emit the data as JSON. Default is ``False``.
    table_rows : list[dict[str, Any]] | None, optional
        The rows to include in the Markdown table. Default is ``None``.
    exit_code : int, optional
        The exit code to return after emitting the data. Default is ``0``.

    Returns
    -------
    int
        The exit code to return after emitting the data.
    """
    HistoryView.validate_output_mode(json_output=json_output, table=table)
    if table:
        _io.emit_markdown_table(
            table_rows
            if table_rows is not None
            else cast(
                list[dict[str, Any]],
                payload,
            ),
            columns=columns,
        )
        return exit_code
    return emit_json_payload(payload, pretty=pretty, exit_code=exit_code)


def start_command(
    *,
    command: str,
    event_format: str | None,
    **fields: Any,
) -> CommandContext:
    """
    Create a command context and emit its started event.

    Parameters
    ----------
    command : str
        The CLI command name (e.g. "run" or "render").
    event_format : str | None
        The requested event output format (e.g. "jsonl" or ``None`` for no
        events).
    **fields : Any
        Additional fields to include in the emitted event payload.

    Returns
    -------
    CommandContext
        The created command context.
    """
    context = CommandContext(
        command=command,
        event_format=event_format,
        run_id=RuntimeEvents.create_run_id(),
        started_at=RuntimeEvents.utc_now_iso(),
        started_perf=perf_counter(),
    )
    emit_lifecycle_event(
        command=context.command,
        lifecycle='started',
        run_id=context.run_id,
        event_format=context.event_format,
        timestamp=context.started_at,
        **fields,
    )
    return context


def complete_command(
    context: CommandContext,
    **fields: Any,
) -> None:
    """
    Emit a completed lifecycle event for one command context.

    Parameters
    ----------
    context : CommandContext
        The command context.
    **fields : Any
        Additional fields to include in the emitted event payload.
    """
    emit_lifecycle_event(
        command=context.command,
        lifecycle='completed',
        run_id=context.run_id,
        event_format=context.event_format,
        duration_ms=elapsed_ms(context.started_perf),
        **fields,
    )


def complete_output(
    context: CommandContext,
    payload: Any,
    *,
    mode: str,
    complete_command_fn: Any,
    pretty: bool = True,
    output_path: str | None = None,
    format_hint: str | None = None,
    success_message: str | None = None,
    **fields: Any,
) -> int:
    """
    Emit command completion and route the payload by output mode.

    Parameters
    ----------
    context : CommandContext
        The command context.
    payload : Any
        The payload to emit.
    mode : str
        The output mode.
    complete_command_fn : Any
        The command completion function.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.
    output_path : str | None, optional
        The path to write the output to. Default is ``None``.
    format_hint : str | None, optional
        The format hint for the output. Default is ``None``.
    success_message : str | None, optional
        The success message to display. Default is ``None``.
    **fields : Any
        Additional fields to include in the emitted event payload.

    Returns
    -------
    int
        The exit code after emitting the output.

    Raises
    ------
    AssertionError
        If an unsupported completion mode is provided.
    """
    complete_command_fn(context, **fields)
    match mode:
        case 'json':
            return emit_json_payload(payload, pretty=pretty)
        case 'or_write':
            _io.emit_or_write(
                payload,
                output_path,
                pretty=pretty,
                success_message=cast(str, success_message),
            )
            return 0
        case 'file':
            target = cast(str, output_path)
            write_file_payload(
                cast(JSONData, payload),
                target,
                format_hint=format_hint,
            )
            print(f'{cast(str, success_message)} {target}')
            return 0
        case 'json_file':
            _io.write_json_output(
                payload,
                cast(str, output_path),
                success_message=cast(str, success_message),
            )
            return 0
        case _:
            raise AssertionError(f'Unsupported completion mode: {mode!r}')


def fail_command(
    context: CommandContext,
    exc: Exception,
    **fields: Any,
) -> None:
    """
    Emit a failed lifecycle event for one command context.

    Parameters
    ----------
    context : CommandContext
        The command context.
    exc : Exception
        The exception that caused the failure.
    **fields : Any
        Additional fields to include in the emitted event payload.
    """
    emit_failure_event(
        command=context.command,
        run_id=context.run_id,
        started_perf=context.started_perf,
        event_format=context.event_format,
        exc=exc,
        **fields,
    )


def record_run_completion(
    history_store: Any,
    context: CommandContext,
    *,
    status: str,
    result_summary: JSONData | None = None,
    exc: Exception | None = None,
) -> None:
    """
    Persist the terminal state for one tracked CLI run.

    Parameters
    ----------
    history_store : Any
        The history store to record the run completion.
    context : CommandContext
        The command context.
    status : str
        The final status of the run (e.g. "ok" or "error").
    result_summary : JSONData | None, optional
        An optional summary of the run results, if any. Default is ``None``.
    exc : Exception | None, optional
        An optional exception raised during the run, if any. Default is
        ``None``.
    """
    history_store.record_run_finished(
        RunCompletion(
            run_id=context.run_id,
            state=RunState(
                status=status,
                finished_at=RuntimeEvents.utc_now_iso(),
                duration_ms=elapsed_ms(context.started_perf),
                result_summary=result_summary,
                error_type=None if exc is None else type(exc).__name__,
                error_message=None if exc is None else str(exc),
            ),
        ),
    )


def resolve_render_template(
    template: TemplateKey | None,
    template_path: str | None,
) -> tuple[TemplateKey | None, str | None]:
    """
    Resolve a key for a template from which to render.

    Parameters
    ----------
    template : TemplateKey | None
        The template key to use, if any. Default is ``None``.
    template_path : str | None
        The path to the template file, if any. Default is ``None``.

    Returns
    -------
    tuple[TemplateKey | None, str | None]
        A tuple containing the resolved template key and file path.
    """
    template_key: TemplateKey | None = template or 'ddl'
    if template_path is not None:
        return template_key, template_path

    candidate_path = Path(cast(str, template_key))
    if candidate_path.exists():
        return None, str(candidate_path)
    return template_key, None


def emit_render_output(
    rendered_chunks: list[str],
    *,
    output_path: str | None,
    pretty: bool,
    quiet: bool,
    schema_count: int,
    print_fn: Any = print,
) -> int:
    """
    Write rendered SQL to a file or STDOUT.

    Parameters
    ----------
    rendered_chunks : list[str]
        The list of rendered SQL chunks.
    output_path : str | None
        The path to the output file, or ``None`` to print to STDOUT.
    pretty : bool
        Whether to pretty-print the SQL output. Default is ``False``.
    quiet : bool
        Whether to suppress informational messages. Default is ``False``.
    schema_count : int
        The number of schemas rendered.
    print_fn : Any, optional
        The function to use for printing output. Default is ``print``.

    Returns
    -------
    int
        The CLI exit code.
    """
    sql_text = '\n'.join(chunk.rstrip() for chunk in rendered_chunks).rstrip() + '\n'
    rendered_output = sql_text if pretty else sql_text.rstrip('\n')
    if output_path and output_path != '-':
        Path(output_path).write_text(rendered_output, encoding='utf-8')
        if not quiet:
            print_fn(f'Rendered {schema_count} schema(s) to {output_path}')
        return 0

    print_fn(rendered_output, end='')
    return 0


def resolve_payload(
    payload: object,
    *,
    format_hint: str | None,
    format_explicit: bool,
    hydrate_files: bool = True,
) -> object:
    """
    Resolve one CLI payload through the shared CLI payload loader.

    Parameters
    ----------
    payload : object
        The CLI payload to resolve.
    format_hint : str | None
        An optional format hint for the payload, if any. Default is ``None``.
    format_explicit : bool
        Whether the format is explicitly specified. Default is ``False``.
    hydrate_files : bool, optional
        Whether to hydrate files referenced in the payload. Default is
        ``True``.

    Returns
    -------
    object
        The resolved payload.
    """
    resolve_kwargs: dict[str, Any] = {
        'format_hint': format_hint,
        'format_explicit': format_explicit,
    }
    if not hydrate_files:
        resolve_kwargs['hydrate_files'] = False
    return _io.resolve_cli_payload(payload, **resolve_kwargs)


def resolve_mapping_payload(
    payload: object,
    *,
    format_explicit: bool,
    error_message: str,
) -> dict[str, Any]:
    """
    Resolve one CLI payload and require a mapping result.

    Parameters
    ----------
    payload : object
        The CLI payload to resolve.
    format_explicit : bool
        Whether the format is explicitly specified.
    error_message : str
        The error message to raise if the resolved payload is not a mapping.

    Returns
    -------
    dict[str, Any]
        The resolved mapping payload.

    Raises
    ------
    ValueError
        If the resolved payload is not a mapping.
    """
    resolved_payload = resolve_payload(
        payload,
        format_hint=None,
        format_explicit=format_explicit,
    )
    if not isinstance(resolved_payload, dict):
        raise ValueError(error_message)
    return resolved_payload


def emit_readiness_report(
    *,
    config: str | None,
    pretty: bool,
    readiness_builder: Any = ReadinessReportBuilder,
    emit_json_payload_fn: Any = emit_json_payload,
) -> int:
    """
    Build and emit one readiness report, returning its CLI exit code..

    Parameters
    ----------
    config : str | None
        The path to the configuration file, or ``None`` to use the default
        configuration.
    pretty : bool
        Whether to pretty-print the JSON output. Default is ``False``.
    readiness_builder : Any
        The readiness report builder to use. Default is
        :class:`ReadinessReportBuilder`.
    emit_json_payload_fn : Any
        The function to use for emitting the JSON payload. Default is
        :func:`emit_json_payload`.

    Returns
    -------
    int
        The CLI exit code.
    """
    report = readiness_builder.build(config_path=config)
    return emit_json_payload_fn(
        report,
        pretty=pretty,
        exit_code=0 if report.get('status') == 'ok' else 1,
    )


def write_file_payload(
    payload: JSONData,
    target: str,
    *,
    format_hint: str | None,
) -> None:
    """
    Write a JSON-like payload to *target* using *format_hint* when given.

    Parameters
    ----------
    payload : JSONData
        The JSON-like payload to write.
    target : str
        The target file path.
    format_hint : str | None
        An optional format hint for the file.
    """
    file_format = FileFormat.coerce(format_hint) if format_hint else None
    File(target, file_format=file_format).write(payload)
