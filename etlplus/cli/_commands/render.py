"""
:mod:`etlplus.cli._commands.render` module.

Typer command for rendering SQL DDL from table schema specs.
"""

from __future__ import annotations

import typer

from .._handlers.render import render_handler
from ._app import app
from ._helpers import call_handler
from ._helpers import require_any
from ._option_specs import RenderConfigOption
from ._option_specs import RenderOutputOption
from ._option_specs import RenderSpecOption
from ._option_specs import RenderTableOption
from ._option_specs import RenderTemplateOption
from ._option_specs import RenderTemplatePathOption
from ._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'render_cmd',
]


# SECTION: FUNCTIONS ======================================================== #


@app.command('render')
def render_cmd(
    ctx: typer.Context,
    config: RenderConfigOption = None,
    spec: RenderSpecOption = None,
    table: RenderTableOption = None,
    template: RenderTemplateOption = 'ddl',
    template_path: RenderTemplatePathOption = None,
    output: RenderOutputOption = None,
) -> int:
    """
    Render SQL DDL from table schemas defined in YAML/JSON configs.

    Parameters
    ----------
    ctx : typer.Context
        Typer context.
    config : RenderConfigOption, optional
        Path to the YAML/JSON config file.
    spec : RenderSpecOption, optional
        Path to a standalone table spec file.
    table : RenderTableOption, optional
        Optional table name filter.
    template : RenderTemplateOption, optional
        Built-in template key to use for rendering.
    template_path : RenderTemplatePathOption, optional
        Custom template path that overrides *template*.
    output : RenderOutputOption, optional
        Optional output path for rendered SQL. Defaults to ``None``.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    require_any(
        (config, spec),
        message="Missing required option '--config' or '--spec'.",
    )

    return call_handler(
        render_handler,
        state=ensure_state(ctx),
        state_fields=('pretty', 'quiet'),
        config=config,
        spec=spec,
        table=table,
        template=template,
        template_path=template_path,
        output=output,
    )
