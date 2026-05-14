"""
:mod:`etlplus.cli._commands.load` module.

Typer command for loading data into supported targets.
"""

from __future__ import annotations

import typer

from ...file import FileFormat
from .._handlers.dataops import load_handler
from ._app import app
from ._constants import FILE_FORMATS
from ._helpers import CommandHelperPolicy
from ._options.common import StructuredEventFormatOption
from ._options.resources import SourceFormatOption
from ._options.resources import TargetArg
from ._options.resources import TargetFormatOption
from ._options.resources import TargetTypeOption
from ._state import ResourceTypeResolver
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
    source_format_hint = (
        None
        if (
            normalized_source_format := ResourceTypeResolver.optional_choice(
                None if source_format is None else str(source_format),
                FILE_FORMATS,
                label='source_format',
            )
        )
        is None
        else FileFormat.coerce(normalized_source_format)
    )
    resolved_target = CommandHelperPolicy.resolve_resource(
        state,
        role='target',
        value=target,
        connector_type=target_type,
        format_value=target_format,
        positional=True,
    )
    resolved_source = CommandHelperPolicy.resolve_resource(
        state,
        role='source',
        value='-',
        soft_inference=True,
    )

    return CommandHelperPolicy.call_handler(
        load_handler,
        state=state,
        source=resolved_source.value,
        source_format=source_format_hint,
        target=resolved_target.value,
        target_type=resolved_target.require_resource_type(),
        target_format=resolved_target.format_hint,
        event_format=event_format,
        output=None,
        format_explicit=resolved_target.format_explicit,
    )
