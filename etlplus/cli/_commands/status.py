"""
:mod:`etlplus.cli._commands.status` module.

Typer command for the latest normalized persisted run.
"""

from __future__ import annotations

import typer

from .._handlers.history import status_handler
from ._app import app
from ._helpers import call_handler
from ._options import JobOption
from ._options import RunIdOption
from ._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'status_cmd',
]


# SECTION: FUNCTIONS ======================================================== #


@app.command('status')
def status_cmd(
    ctx: typer.Context,
    job: JobOption = None,
    run_id: RunIdOption = None,
) -> int:
    """
    Inspect the latest normalized persisted run.

    Parameters
    ----------
    ctx : typer.Context
        Typer context.
    job : JobOption, optional
        Specific job to inspect (defaults to the latest job).
    run_id : RunIdOption, optional
        Specific run ID to inspect (defaults to the latest run).

    Returns
    -------
    int
        Exit code (0 if checks passed, non-zero if any checks failed).
    """
    return call_handler(
        status_handler,
        state=ensure_state(ctx),
        job=job,
        run_id=run_id,
    )
