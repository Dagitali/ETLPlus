"""
:mod:`etlplus.cli._commands.run` module.

Typer command for executing configured jobs and pipelines.
"""

from __future__ import annotations

import typer

from .. import _handlers as handlers
from ._app import app
from ._helpers import call_handler
from ._helpers import require_value
from ._options import ConfigOption
from ._options import JobOption
from ._options import PipelineOption
from ._options import StructuredEventFormatOption
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
        Path to YAML/JSON config file.
    job : JobOption, optional
        Specific job to execute (defaults to all jobs in the config).
    pipeline : PipelineOption, optional
        Specific pipeline to execute (defaults to all pipelines in the config).
    event_format : StructuredEventFormatOption, optional
        Format for structured events (defaults to None).

    Returns
    -------
    int
        Exit code (0 if checks passed, non-zero if any checks failed).
    """
    config = require_value(
        config,
        message="Missing required option '--config'.",
    )

    return call_handler(
        handlers.run_handler,
        state=ensure_state(ctx),
        config=config,
        job=job,
        pipeline=pipeline,
        event_format=event_format,
    )
