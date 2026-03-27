"""
:mod:`etlplus.cli._handlers.common` module.

Shared command lifecycle and output helpers for CLI handlers.
"""

from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Any

from ...file import File
from ...file import FileFormat
from ...runtime.events import RuntimeEvents
from ...utils.types import JSONData
from .. import _io

# SECTION: EXPORTS ========================================================== #


# __all__ = [
#     # Functions
# ]


# SECTION: INTERNAL DATA CLASSES ============================================ #


@dataclass(frozen=True, slots=True)
class _CommandContext:
    """Shared runtime context for one CLI command invocation."""

    command: str
    event_format: str | None
    run_id: str
    started_at: str
    started_perf: float


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _elapsed_ms(started_perf: float) -> int:
    """Return elapsed milliseconds since *started_perf*."""
    return int((perf_counter() - started_perf) * 1000)


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
        duration_ms=_elapsed_ms(started_perf),
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


def _complete_command(context: _CommandContext, **fields: Any) -> None:
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


def _write_file_payload(
    payload: JSONData,
    target: str,
    *,
    format_hint: str | None,
) -> None:
    """Write a JSON-like payload to a file path using an optional format hint."""
    file_format = FileFormat.coerce(format_hint) if format_hint else None
    File(target, file_format=file_format).write(payload)


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
            table_rows if table_rows is not None else payload,
            columns=columns,
            exit_code=exit_code,
        )
    return _emit_json_payload(payload, pretty=pretty, exit_code=exit_code)
