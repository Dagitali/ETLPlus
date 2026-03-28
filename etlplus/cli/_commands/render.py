"""
:mod:`etlplus.cli._commands.render` module.

Typer command for rendering SQL DDL from table schema specs.
"""

from __future__ import annotations

import typer

from .. import _handlers as handlers
from ._helpers import call_handler
from ._helpers import require_any
from ._state import ensure_state
from .app import app
from .options import OutputOption
from .options import RenderConfigOption
from .options import RenderSpecOption
from .options import RenderTableOption
from .options import RenderTemplateOption
from .options import RenderTemplatePathOption

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
    output: OutputOption = None,
) -> int:
    """
    Render SQL DDL from table schemas defined in YAML/JSON configs.

    Parameters
    ----------
    ctx : typer.Context
        Typer context.
    config : RenderConfigOption, optional
        Path to YAML/JSON config file.
    spec : RenderSpecOption, optional
        YAML/JSON string containing table schema definitions.
    table : RenderTableOption, optional
        Specific table to render (defaults to all tables in the config/spec).
    template : RenderTemplateOption, optional
        Name of the Jinja2 template to use for rendering (defaults to 'ddl').
    template_path : RenderTemplatePathOption, optional
        Path to a custom Jinja2 template file to use for rendering (overrides
        the `--template` option).
    output : OutputOption, optional
        Path to output file for rendered SQL (defaults to stdout).

    Returns
    -------
    int
        Exit code (0 if checks passed, non-zero if any checks failed).
    """
    require_any(
        (config, spec),
        message="Missing required option '--config' or '--spec'.",
    )

    return call_handler(
        handlers.render_handler,
        state=ensure_state(ctx),
        state_fields=('pretty', 'quiet'),
        config=config,
        spec=spec,
        table=table,
        template=template,
        template_path=template_path,
        output=output,
    )
