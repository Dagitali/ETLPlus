"""
:mod:`etlplus.cli._commands.run` module.

Typer command for executing configured jobs and pipelines.
"""

from __future__ import annotations

import typer

from .._handlers.run import run_handler
from ._app import app
from ._helpers import call_handler
from ._helpers import fail_usage
from ._helpers import require_value
from ._options.common import ConfigOption
from ._options.common import ContinueOnFailOption
from ._options.common import JobOption
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
    event_format : StructuredEventFormatOption, optional
        Structured event output format.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    config = require_value(
        config,
        message="Missing required option '--config'.",
    )
    if run_all and (job or pipeline):
        fail_usage('--all cannot be combined with --job or --pipeline.')

    return call_handler(
        run_handler,
        state=ensure_state(ctx),
        config=config,
        job=job,
        pipeline=pipeline,
        run_all=run_all,
        continue_on_fail=continue_on_fail,
        event_format=event_format,
    )
