"""
:mod:`etlplus.cli._commands.run` module.

Typer command for executing configured jobs and pipelines.
"""

from __future__ import annotations

import typer

from .._handlers.run import run_handler
from ._app import app
from ._helpers import CommandHelperPolicy
from ._options.common import CaptureTracebacksOption
from ._options.common import ConfigOption
from ._options.common import ContinueOnFailOption
from ._options.common import HistoryBackendOption
from ._options.common import HistoryEnabledOption
from ._options.common import HistoryStateDirOption
from ._options.common import JobOption
from ._options.common import MaxConcurrencyOption
from ._options.common import PipelineOption
from ._options.common import RunAllOption
from ._options.common import StructuredEventFormatOption
from ._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'run_cmd',
]


# SECTION: FUNCTIONS ======================================================== #


@app.command('run')
def run_cmd(
    ctx: typer.Context,
    config: ConfigOption,
    job: JobOption = None,
    pipeline: PipelineOption = None,
    run_all: RunAllOption = False,
    continue_on_fail: ContinueOnFailOption = False,
    max_concurrency: MaxConcurrencyOption = None,
    history_enabled: HistoryEnabledOption = None,
    history_backend: HistoryBackendOption = None,
    history_state_dir: HistoryStateDirOption = None,
    capture_tracebacks: CaptureTracebacksOption = None,
    event_format: StructuredEventFormatOption = None,
) -> int:
    """
    Execute an ETL job or pipeline from a YAML configuration.

    Parameters
    ----------
    ctx : typer.Context
        Typer context.
    config : ConfigOption
        Path to the YAML/JSON config file.
    job : JobOption, optional
        Job name to execute.
    pipeline : PipelineOption, optional
        Pipeline name to execute when *job* is not provided.
    run_all : RunAllOption, optional
        Whether to run all configured jobs in DAG order.
    continue_on_fail : ContinueOnFailOption, optional
        Whether to continue past failed jobs and skip only blocked downstream
        jobs.
    max_concurrency : MaxConcurrencyOption, optional
        Maximum number of independent DAG jobs to run concurrently.
    history_enabled : HistoryEnabledOption, optional
        Override local run-history persistence for this invocation.
    history_backend : HistoryBackendOption, optional
        Override the local history backend for this invocation.
    history_state_dir : HistoryStateDirOption, optional
        Override the local history state directory for this invocation.
    capture_tracebacks : CaptureTracebacksOption, optional
        Override whether capped failure tracebacks are persisted in local
        run history.
    event_format : StructuredEventFormatOption, optional
        Structured event output format.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    config = CommandHelperPolicy.require_value(
        config,
        message="Missing required option '--config'.",
    )
    if run_all and (job or pipeline):
        CommandHelperPolicy.fail_usage(
            '--all cannot be combined with --job or --pipeline.',
        )

    return CommandHelperPolicy.call_handler(
        run_handler,
        state=ensure_state(ctx),
        config=config,
        job=job,
        pipeline=pipeline,
        run_all=run_all,
        continue_on_fail=continue_on_fail,
        max_concurrency=max_concurrency,
        history_enabled=history_enabled,
        history_backend=history_backend,
        history_state_dir=history_state_dir,
        capture_tracebacks=capture_tracebacks,
        event_format=event_format,
    )
