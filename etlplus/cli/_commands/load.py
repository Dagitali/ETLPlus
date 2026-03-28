"""
:mod:`etlplus.cli._commands.load` module.

Typer command for loading data into supported targets.
"""

from __future__ import annotations

import typer

from .. import _handlers as handlers
from ._app import app
from ._helpers import call_handler
from ._helpers import normalize_file_format
from ._helpers import resolve_resource
from ._options import SourceFormatOption
from ._options import StructuredEventFormatOption
from ._options import TargetArg
from ._options import TargetFormatOption
from ._options import TargetTypeOption
from ._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'load_cmd',
]


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
    source_format_hint = normalize_file_format(
        source_format,
        label='source_format',
    )
    resolved_target = resolve_resource(
        state,
        role='target',
        value=target,
        connector_type=target_type,
        format_value=target_format,
        positional=True,
    )
    assert resolved_target.resource_type is not None
    resolved_source = resolve_resource(
        state,
        role='source',
        value='-',
        soft_inference=True,
    )

    return call_handler(
        handlers.load_handler,
        state=state,
        source=resolved_source.value,
        target_type=resolved_target.resource_type,
        target=resolved_target.value,
        event_format=event_format,
        source_format=source_format_hint,
        target_format=resolved_target.format_hint,
        format_explicit=resolved_target.format_hint is not None,
        output=None,
    )
