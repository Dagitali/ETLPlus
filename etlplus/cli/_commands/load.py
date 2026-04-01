"""
:mod:`etlplus.cli._commands.load` module.

Typer command for loading data into supported targets.
"""

from __future__ import annotations

import typer

from .._handlers.dataops import load_handler
from ._app import app
from ._helpers import call_handler
from ._helpers import normalize_file_format
from ._helpers import resolve_resource
from ._options.common import StructuredEventFormatOption
from ._options.resources import SourceFormatOption
from ._options.resources import TargetArg
from ._options.resources import TargetFormatOption
from ._options.resources import TargetTypeOption
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
        Source payload format override for STDIN.
    target : TargetArg, optional
        Target path, URI/URL, or ``-`` for STDOUT.
    target_format : TargetFormatOption, optional
        Target payload format override.
    target_type : TargetTypeOption, optional
        Target connector type override.
    event_format : StructuredEventFormatOption, optional
        Structured event output format.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
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
        load_handler,
        state=state,
        source=resolved_source.value,
        source_format=source_format_hint,
        target=resolved_target.value,
        target_type=resolved_target.resource_type,
        target_format=resolved_target.format_hint,
        event_format=event_format,
        output=None,
        format_explicit=resolved_target.format_hint is not None,
    )
