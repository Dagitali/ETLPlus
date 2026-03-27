"""
:mod:`etlplus.cli._commands.run` module.

Typer command for executing configured jobs and pipelines.
"""

from __future__ import annotations

import typer

from etlplus.cli import _handlers as handlers
from etlplus.cli._commands.app import app
from etlplus.cli._commands.helpers import require_option
from etlplus.cli._commands.options import ConfigOption
from etlplus.cli._commands.options import JobOption
from etlplus.cli._commands.options import PipelineOption
from etlplus.cli._commands.options import StructuredEventFormatOption
from etlplus.cli._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = ['run_cmd']


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
    config = require_option(config, flag='--config')

    state = ensure_state(ctx)
    return int(
        handlers.run_handler(
            config=config,
            job=job,
            pipeline=pipeline,
            event_format=event_format,
            pretty=state.pretty,
        ),
    )
