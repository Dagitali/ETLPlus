"""
:mod:`etlplus.cli._handlers._lifecycle` module.

Lifecycle helpers shared by CLI handler implementations.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from time import perf_counter
from traceback import format_exception
from typing import Any

from ...history import RunCompletion
from ...history import RunState
from ...runtime import RuntimeEvents
from ...utils._types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'CommandContext',
    # Functions
    'complete_command',
    'elapsed_ms',
    'emit_failure_event',
    'emit_lifecycle_event',
    'fail_command',
    'failure_boundary',
    'record_run_completion',
    'start_command',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_TRACEBACK_CHAR_LIMIT = 16_000


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


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _capture_traceback(
    exc: Exception,
) -> str:
    """Return one capped traceback string for a persisted run failure."""
    traceback_text = ''.join(
        format_exception(type(exc), exc, exc.__traceback__),
    )
    if len(traceback_text) <= _TRACEBACK_CHAR_LIMIT:
        return traceback_text
    suffix = '\n...[truncated]\n'
    return traceback_text[: _TRACEBACK_CHAR_LIMIT - len(suffix)] + suffix


# SECTION: FUNCTIONS ======================================================== #


def elapsed_ms(started_perf: float) -> int:
    """Return elapsed milliseconds since *started_perf*."""
    return int((perf_counter() - started_perf) * 1000)


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
        The name of the command being observed, e.g. "check", "run", etc.
    lifecycle : str
        The lifecycle stage being observed, e.g. "started", "completed", or
        "failed".
    run_id : str
        The unique identifier for the observed command invocation.
    event_format : str | None
        The format to use when emitting the event, or ``None`` to use the
        default format configured for the runtime environment.
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
    Emit a failed command lifecycle event with the stable shared schema.

    Parameters
    ----------
    command : str
        The name of the command being observed, e.g. "check", "run", etc.
    run_id : str
        The unique identifier for the observed command invocation.
    started_perf : float
        The performance counter value at the start of the command.
    event_format : str | None
        The format to use when emitting the event, or ``None`` to use the
        default format configured for the runtime environment.
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
        The name of the command being started, e.g. "check", "run", etc.
    event_format : str | None
        The format to use when emitting the event, or ``None`` to use the
        default format configured for the runtime environment.
    **fields : Any
        Additional fields to include in the emitted event payload.

    Returns
    -------
    CommandContext
        The created command context for the started command.
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
    Emit a completed lifecycle event for *context*.

    Parameters
    ----------
    context : CommandContext
        The command context for the completed command.
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


def fail_command(
    context: CommandContext,
    exc: Exception,
    **fields: Any,
) -> None:
    """
    Emit a failed lifecycle event for *context*.

    Parameters
    ----------
    context : CommandContext
        The command context for the failed command.
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


@contextmanager
def failure_boundary(
    context: CommandContext,
    *,
    on_error: Callable[[Exception], None] | None = None,
    **fields: Any,
) -> Iterator[None]:
    """
    Emit a failed lifecycle event for exceptions raised inside the block.

    Parameters
    ----------
    context : CommandContext
        The command context for the failed command.
    on_error : Callable[[Exception], None] | None, optional
        A callback to invoke with the exception if one is raised.
    **fields : Any
        Additional fields to include in the emitted event payload.

    Raises
    ------
    Exception
        Any exception raised inside the block will be re-raised after emitting
        the failure event and invoking the callback.
    """
    try:
        yield
    except Exception as exc:
        if on_error is not None:
            on_error(exc)
        fail_command(context, exc, **fields)
        raise


def record_run_completion(
    history_store: Any,
    context: CommandContext,
    *,
    status: str,
    result_summary: JSONData | None = None,
    exc: Exception | None = None,
    capture_tracebacks: bool = False,
    error_message: str | None = None,
    error_traceback: str | None = None,
    error_type: str | None = None,
) -> None:
    """
    Persist the terminal state for one tracked CLI run.

    Parameters
    ----------
    history_store : Any
        The history store to record the run completion.
    context : CommandContext
        The command context for the completed command.
    status : str
        The final status of the run.
    result_summary : JSONData | None, optional
        A summary of the run's result, if available.
    exc : Exception | None, optional
        The exception that caused the failure, if any.
    capture_tracebacks : bool, optional
        Whether to persist a capped traceback string for exception failures.
    error_message : str | None, optional
        Explicit error message for handled failures without an exception.
    error_traceback : str | None, optional
        Explicit error traceback for handled failures without an exception.
    error_type : str | None, optional
        Explicit error type for handled failures without an exception.
    """
    history_store.record_run_finished(
        RunCompletion(
            run_id=context.run_id,
            state=RunState(
                status=status,
                finished_at=RuntimeEvents.utc_now_iso(),
                duration_ms=elapsed_ms(context.started_perf),
                result_summary=result_summary,
                error_type=(
                    error_type
                    if error_type is not None
                    else None
                    if exc is None
                    else type(exc).__name__
                ),
                error_message=(
                    error_message
                    if error_message is not None
                    else None
                    if exc is None
                    else str(exc)
                ),
                error_traceback=(
                    error_traceback
                    if error_traceback is not None
                    else _capture_traceback(exc)
                    if capture_tracebacks and exc is not None
                    else None
                ),
            ),
        ),
    )
