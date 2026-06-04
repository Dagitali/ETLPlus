"""Local web UI Typer option aliases for CLI command modules."""

from __future__ import annotations

from .helpers import typer_flag_option_alias
from .helpers import typer_value_option_alias

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'UiHostOption',
    'UiLimitOption',
    'UiNoBrowserOption',
    'UiPortOption',
    'UiRefreshSecondsOption',
]


# SECTION: TYPES ============================================================ #


UiHostOption = typer_value_option_alias(
    str,
    '--host',
    help_text='Host interface for the local web UI.',
    show_default=True,
)

UiLimitOption = typer_value_option_alias(
    int,
    '--limit',
    help_text='Maximum number of run and job history rows to show.',
    min=1,
    show_default=True,
)

UiNoBrowserOption = typer_flag_option_alias(
    '--no-browser',
    help_text='Do not open the UI automatically in a browser.',
)

UiPortOption = typer_value_option_alias(
    int,
    '--port',
    help_text='TCP port for the local web UI.',
    min=1,
    max=65535,
    show_default=True,
)

UiRefreshSecondsOption = typer_value_option_alias(
    int,
    '--refresh-seconds',
    help_text='Page refresh interval in seconds. Use 0 to disable refresh.',
    min=0,
    show_default=True,
)
