"""
:mod:`etlplus.cli._commands.check` module.

Typer command for inspecting pipeline configurations.
"""

from __future__ import annotations

import typer

from .. import _handlers as handlers
from .._state import ensure_state
from .app import app
from .helpers import _call_handler
from .helpers import fail_usage
from .helpers import require_option
from .options import CheckConfigOption
from .options import JobsOption
from .options import PipelinesOption
from .options import ReadinessOption
from .options import SourcesOption
from .options import SummaryOption
from .options import TargetsOption
from .options import TransformsOption

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

    """
    inspection_requested = any(
        (jobs, pipelines, sources, summary, targets, transforms),
    )

    if readiness and inspection_requested:
        fail_usage('--readiness cannot be combined with inspection flags.')

    if not readiness and not config:
        require_option(config, flag='--config')

    return _call_handler(
        handlers.check_handler,
        state=ensure_state(ctx),
        config=config,
        jobs=jobs,
        pipelines=pipelines,
        readiness=readiness,
        sources=sources,
        summary=summary,
        targets=targets,
        transforms=transforms,
    )
