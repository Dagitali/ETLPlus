"""
:mod:`etlplus.cli._commands._option_history` module.

History and report Typer option aliases for CLI command modules.
"""

from __future__ import annotations

from typing import Annotated
from typing import Literal

import typer

from ._option_helpers import _typer_flag_option_kwargs
from ._option_helpers import _typer_timestamp_option_kwargs
from ._option_helpers import _typer_value_option_kwargs

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'HistoryFollowOption',
    'HistoryJsonOption',
    'HistoryLimitOption',
    'HistoryRawOption',
    'HistorySinceOption',
    'HistoryStatusOption',
    'HistoryTableOption',
    'HistoryUntilOption',
    'ReportGroupByOption',
    'RunIdOption',
]


# SECTION: TYPES ============================================================ #


HistoryFollowOption = Annotated[
    bool,
    typer.Option(
        '--follow',
        **_typer_flag_option_kwargs(
            'Keep polling for newly persisted matching raw history events.',
        ),
    ),
]

HistoryJsonOption = Annotated[
    bool,
    typer.Option(
        '--json',
        **_typer_flag_option_kwargs('Format output as JSON explicitly.'),
    ),
]

HistoryLimitOption = Annotated[
    int | None,
    typer.Option(
        '--limit',
        min=1,
        **_typer_value_option_kwargs(
            'Maximum number of history records to emit.',
        ),
    ),
]

HistoryRawOption = Annotated[
    bool,
    typer.Option(
        '--raw',
        **_typer_flag_option_kwargs(
            'Emit raw append events instead of normalized runs.',
        ),
    ),
]

HistorySinceOption = Annotated[
    str | None,
    typer.Option(
        '--since',
        **_typer_timestamp_option_kwargs(bound='since'),
    ),
]

HistoryStatusOption = Annotated[
    str | None,
    typer.Option(
        '--status',
        **_typer_value_option_kwargs('Filter persisted runs by status.'),
    ),
]

HistoryTableOption = Annotated[
    bool,
    typer.Option(
        '--table',
        **_typer_flag_option_kwargs(
            'Format normalized history output as a Markdown table.',
        ),
    ),
]

HistoryUntilOption = Annotated[
    str | None,
    typer.Option(
        '--until',
        **_typer_timestamp_option_kwargs(bound='until'),
    ),
]

ReportGroupByOption = Annotated[
    Literal['day', 'job', 'status'],
    typer.Option(
        '--group-by',
        **_typer_value_option_kwargs(
            'Grouping dimension for aggregated history reports.',
            show_default=True,
        ),
    ),
]

RunIdOption = Annotated[
    str | None,
    typer.Option(
        '--run-id',
        **_typer_value_option_kwargs('Filter persisted runs by run identifier.'),
    ),
]
