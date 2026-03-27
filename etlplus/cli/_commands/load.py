"""
:mod:`etlplus.cli._commands.load` module.

Typer command for loading data into supported targets.
"""

from __future__ import annotations

import typer

from .. import _handlers as handlers
from .._state import ensure_state
from .app import app
from .helpers import normalize_file_format
from .helpers import normalize_resource_type
from .helpers import require_positional_argument
from .helpers import resolve_logged_resource_type
from .options import SourceFormatOption
from .options import StructuredEventFormatOption
from .options import TargetArg
from .options import TargetFormatOption
from .options import TargetTypeOption

# SECTION: EXPORTS ========================================================== #


__all__ = ['load_cmd']


# SECTION: FUNCTIONS ======================================================== #


@app.command('load')
def load_cmd(
    ctx: typer.Context,
    source_format: SourceFormatOption = None,
    target: TargetArg = '-',
    target_format: TargetFormatOption = None,
    target_type: TargetTypeOption = None,
    event_format: StructuredEventFormatOption = None,
) -> int:
    """
    Load data into a file, database, or REST API.

    Parameters
    ----------
    ctx : typer.Context
        Typer context.
    source_format : SourceFormatOption, optional
        Format of the source data.
    target : TargetArg, optional
        Target to load data into.
    target_format : TargetFormatOption, optional
        Format of the target data.
    target_type : TargetTypeOption, optional
        Type of the target.
    event_format : StructuredEventFormatOption, optional
        Format of structured events.

    Returns
    -------
    int
        Exit code (0 if checks passed, non-zero if any checks failed).
    """
    state = ensure_state(ctx)
    target = require_positional_argument(target, name='TARGET')

    source_format = normalize_file_format(
        source_format,
        label='source_format',
    )
    target_type = normalize_resource_type(
        target_type,
        label='target_type',
    )
    target_format = normalize_file_format(
        target_format,
        label='target_format',
    )

    resolved_target = target
    resolved_target_type = resolve_logged_resource_type(
        state,
        role='target',
        value=resolved_target,
        explicit_type=target_type,
    )
    assert resolved_target_type is not None
    resolved_source_value = '-'
    resolve_logged_resource_type(
        state,
        role='source',
        value=resolved_source_value,
        explicit_type=None,
        soft_inference=True,
    )

    return int(
        handlers.load_handler(
            source=resolved_source_value,
            target_type=resolved_target_type,
            target=resolved_target,
            event_format=event_format,
            source_format=source_format,
            target_format=target_format,
            format_explicit=target_format is not None,
            output=None,
            pretty=state.pretty,
        ),
    )
