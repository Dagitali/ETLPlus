"""
:mod:`etlplus.cli._commands.load` module.

Typer command for loading data into supported targets.
"""

from __future__ import annotations

from typing import cast

import typer

from etlplus.cli import _handlers as handlers
from etlplus.cli._commands.app import app
from etlplus.cli._commands.helpers import normalize_choice
from etlplus.cli._commands.helpers import require_positional_argument
from etlplus.cli._commands.options import SourceFormatOption
from etlplus.cli._commands.options import StructuredEventFormatOption
from etlplus.cli._commands.options import TargetArg
from etlplus.cli._commands.options import TargetFormatOption
from etlplus.cli._commands.options import TargetTypeOption
from etlplus.cli._constants import DATA_CONNECTORS
from etlplus.cli._constants import FILE_FORMATS
from etlplus.cli._state import ResourceTypeResolver
from etlplus.cli._state import ensure_state
from etlplus.cli._state import log_inferred_resource

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

    source_format = cast(
        SourceFormatOption,
        normalize_choice(
            source_format,
            FILE_FORMATS,
            label='source_format',
        ),
    )
    target_type = normalize_choice(
        target_type,
        DATA_CONNECTORS,
        label='target_type',
    )
    target_format = cast(
        TargetFormatOption,
        normalize_choice(
            target_format,
            FILE_FORMATS,
            label='target_format',
        ),
    )

    resolved_target = target
    resolved_target_type = target_type or ResourceTypeResolver.infer_or_exit(
        resolved_target,
    )

    resolved_source_value = '-'
    resolved_source_type = ResourceTypeResolver.infer_soft(resolved_source_value)

    log_inferred_resource(
        state,
        role='source',
        value=resolved_source_value,
        resource_type=resolved_source_type,
    )
    log_inferred_resource(
        state,
        role='target',
        value=resolved_target,
        resource_type=resolved_target_type,
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
