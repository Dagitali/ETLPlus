"""
:mod:`etlplus.cli._commands.run` module.

Typer command for executing configured jobs and pipelines.
"""

from __future__ import annotations

import typer

from .._handlers.run import run_handler
from ._app import app
from ._helpers import call_handler
from ._helpers import require_value
from ._option_common import ConfigOption
from ._option_common import JobOption
from ._option_common import PipelineOption
from ._option_common import StructuredEventFormatOption
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

    return call_handler(
        run_handler,
        state=ensure_state(ctx),
        config=config,
        job=job,
        pipeline=pipeline,
        event_format=event_format,
    )
