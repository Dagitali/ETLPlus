"""
:mod:`etlplus.cli._commands.ui` module.

Typer command for the local ETLPlus web UI.
"""

from __future__ import annotations

from etlplus.history._ui import DEFAULT_UI_HOST
from etlplus.history._ui import DEFAULT_UI_LIMIT
from etlplus.history._ui import DEFAULT_UI_PORT
from etlplus.history._ui import DEFAULT_UI_REFRESH_SECONDS
from etlplus.history._ui import serve_history_ui

from ._app import app
from ._options.ui import UiHostOption
from ._options.ui import UiLimitOption
from ._options.ui import UiNoBrowserOption
from ._options.ui import UiPortOption
from ._options.ui import UiRefreshSecondsOption

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'ui_cmd',
]


# SECTION: FUNCTIONS ======================================================== #


@app.command('ui')
def ui_cmd(
    host: UiHostOption = DEFAULT_UI_HOST,
    port: UiPortOption = DEFAULT_UI_PORT,
    limit: UiLimitOption = DEFAULT_UI_LIMIT,
    refresh_seconds: UiRefreshSecondsOption = DEFAULT_UI_REFRESH_SECONDS,
    no_browser: UiNoBrowserOption = False,
) -> int:
    """
    Serve a read-only local web UI for persisted run history.

    Parameters
    ----------
    host : UiHostOption, optional
        Host interface for the local web UI.
    port : UiPortOption, optional
        TCP port for the local web UI.
    limit : UiLimitOption, optional
        Maximum number of run and job history rows to show.
    refresh_seconds : UiRefreshSecondsOption, optional
        Page refresh interval in seconds. Use ``0`` to disable refresh.
    no_browser : UiNoBrowserOption, optional
        Whether to skip opening the default browser automatically.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    return serve_history_ui(
        host=host,
        port=port,
        limit=limit,
        refresh_seconds=refresh_seconds,
        open_browser=not no_browser,
    )
