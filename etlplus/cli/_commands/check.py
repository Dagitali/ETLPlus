"""
:mod:`etlplus.cli._commands.check` module.

Typer command for inspecting pipeline configurations.
"""

from __future__ import annotations

import typer

from etlplus.cli import _handlers as handlers
from etlplus.cli._commands.app import app
from etlplus.cli._commands.options import CheckConfigOption
from etlplus.cli._commands.options import JobsOption
from etlplus.cli._commands.options import PipelinesOption
from etlplus.cli._commands.options import ReadinessOption
from etlplus.cli._commands.options import SourcesOption
from etlplus.cli._commands.options import SummaryOption
from etlplus.cli._commands.options import TargetsOption
from etlplus.cli._commands.options import TransformsOption
from etlplus.cli._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = ['check_cmd']


# SECTION: FUNCTIONS ======================================================== #


@app.command('check')
def check_cmd(
    ctx: typer.Context,
    config: CheckConfigOption = None,
    jobs: JobsOption = False,
    pipelines: PipelinesOption = False,
    readiness: ReadinessOption = False,
    sources: SourcesOption = False,
    summary: SummaryOption = False,
    targets: TargetsOption = False,
    transforms: TransformsOption = False,
) -> int:
    """
    Inspect a pipeline configuration.

    By default, this command performs a basic sanity check of the provided
    configuration. Use the various inspection flags to get more detailed info
    on specific aspects of the configuration. The `--readiness` flag performs a
    more thorough check of the configuration's readiness for execution, but it
    cannot be combined with the other inspection flags.

    Parameters
    ----------
    ctx : typer.Context
        Typer context.
    config : CheckConfigOption, optional
        Path to YAML/JSON config file.
    jobs : JobsOption, optional
        Whether to inspect job definitions.
    pipelines : PipelinesOption, optional
        Whether to inspect pipeline definitions.
    readiness : ReadinessOption, optional
        Whether to perform a readiness check.
    sources : SourcesOption, optional
        Whether to inspect source definitions.
    summary : SummaryOption, optional
        Whether to print a summary of the configuration.
    targets : TargetsOption, optional
        Whether to inspect target definitions.
    transforms : TransformsOption, optional
        Whether to inspect transform definitions.

    Returns
    -------
    int
        Exit code (0 if checks passed, non-zero if any checks failed).

    Raises
    ------
    typer.Exit
        If the provided options are invalid or if required options are missing.
    """
    inspection_requested = any((jobs, pipelines, sources, summary, targets, transforms))

    if readiness and inspection_requested:
        typer.echo(
            'Error: --readiness cannot be combined with inspection flags.',
            err=True,
        )
        raise typer.Exit(2)

    if not readiness and not config:
        typer.echo("Error: Missing required option '--config'.", err=True)
        raise typer.Exit(2)

    state = ensure_state(ctx)
    return int(
        handlers.check_handler(
            config=config,
            jobs=jobs,
            pipelines=pipelines,
            readiness=readiness,
            sources=sources,
            summary=summary,
            targets=targets,
            transforms=transforms,
            pretty=state.pretty,
        ),
    )
