"""
:mod:`etlplus.cli._commands.check` module.

Typer command for inspecting pipeline configurations.
"""

from __future__ import annotations

import typer

from .._handlers.check import check_handler
from ._app import app
from ._helpers import call_handler
from ._helpers import fail_usage
from ._helpers import require_value
from ._options.common import CheckConfigOption
from ._options.specs import JobsOption
from ._options.specs import PipelinesOption
from ._options.specs import ReadinessOption
from ._options.specs import SourcesOption
from ._options.specs import StrictOption
from ._options.specs import SummaryOption
from ._options.specs import TargetsOption
from ._options.specs import TransformsOption
from ._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'check_cmd',
]


# SECTION: FUNCTIONS ======================================================== #


@app.command('check')
def check_cmd(
    ctx: typer.Context,
    config: CheckConfigOption = None,
    jobs: JobsOption = False,
    pipelines: PipelinesOption = False,
    readiness: ReadinessOption = False,
    sources: SourcesOption = False,
    strict: StrictOption = False,
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
        Path to the YAML/JSON config file.
    jobs : JobsOption, optional
        Whether to inspect job definitions.
    pipelines : PipelinesOption, optional
        Whether to inspect pipeline definitions.
    readiness : ReadinessOption, optional
        Whether to perform a readiness check.
    sources : SourcesOption, optional
        Whether to inspect source definitions.
    strict : StrictOption, optional
        Whether to enable strict config diagnostics.
    summary : SummaryOption, optional
        Whether to print a configuration summary.
    targets : TargetsOption, optional
        Whether to inspect target definitions.
    transforms : TransformsOption, optional
        Whether to inspect transform definitions.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    inspection_requested = any(
        (jobs, pipelines, sources, summary, targets, transforms),
    )

    if readiness and inspection_requested:
        fail_usage('--readiness cannot be combined with inspection flags.')

    if not readiness and not config:
        require_value(
            config,
            message="Missing required option '--config'.",
        )

    return call_handler(
        check_handler,
        state=ensure_state(ctx),
        config=config,
        jobs=jobs,
        pipelines=pipelines,
        readiness=readiness,
        sources=sources,
        strict=strict,
        summary=summary,
        targets=targets,
        transforms=transforms,
    )
