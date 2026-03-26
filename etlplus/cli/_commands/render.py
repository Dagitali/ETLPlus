"""
:mod:`etlplus.cli._commands.render` module.

Typer command for rendering SQL DDL from table schema specs.
"""

from __future__ import annotations

import typer

from etlplus.cli import _handlers as handlers
from etlplus.cli._commands.app import app
from etlplus.cli._commands.options import OutputOption
from etlplus.cli._commands.options import RenderConfigOption
from etlplus.cli._commands.options import RenderSpecOption
from etlplus.cli._commands.options import RenderTableOption
from etlplus.cli._commands.options import RenderTemplateOption
from etlplus.cli._commands.options import RenderTemplatePathOption
from etlplus.cli._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = ['render_cmd']


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

    Raises
    ------
    typer.Exit
        If the provided options are invalid or if required options are missing.
    """
    if not (config or spec):
        typer.echo(
            "Error: Missing required option '--config' or '--spec'.",
            err=True,
        )
        raise typer.Exit(2)

    state = ensure_state(ctx)
    return int(
        handlers.render_handler(
            config=config,
            spec=spec,
            table=table,
            template=template,
            template_path=template_path,
            output=output,
            pretty=state.pretty,
            quiet=state.quiet,
        ),
    )
