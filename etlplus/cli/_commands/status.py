"""
:mod:`etlplus.cli._commands.status` module.

Typer command for the latest normalized persisted run.
"""

from __future__ import annotations

import typer

from .._handlers.history import status_handler
from ._app import app
from ._helpers import call_handler
from ._options.common import JobOption
from ._options.history import RunIdOption
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
        Specific job to inspect. Defaults to ``None`` for the latest job).
    run_id : RunIdOption, optional
        Specific run ID to inspect. Defaults to ``None`` for the latest run).

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    return call_handler(
        status_handler,
        state=ensure_state(ctx),
        job=job,
        run_id=run_id,
    )
