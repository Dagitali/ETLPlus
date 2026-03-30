"""
:mod:`etlplus.cli._handler_render` module.

SQL render handler implementation for the CLI facade.
"""

from __future__ import annotations

from typing import Any

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'render_handler',
]


# SECTION: FUNCTIONS ======================================================== #


def render_handler(
    *,
    config: str | None = None,
    spec: str | None = None,
    table: str | None = None,
    template: Any = None,
    template_path: str | None = None,
    output: str | None = None,
    pretty: bool = True,
    quiet: bool = False,
    resolve_render_template_fn: Any,
    summary_module: Any,
    render_tables_fn: Any,
    emit_render_output_fn: Any,
    print_fn: Any,
    stderr: Any,
) -> int:
    """
    Render SQL DDL statements from table schema specs.

    Parameters
    ----------
    config : str | None, optional
        Optional path to a pipeline config file containing table schema specs.
    spec : str | None, optional
        Optional path to a single table schema spec file. If provided, this
        takes precedence over any specs in a config file.
    table : str | None, optional
        Optional table name to filter specs by. Matches against the 'table' or
        'name' field of specs. If provided, only specs matching this table name
        are rendered.
    template : Any, optional
        Optional template name or object to use for rendering. If not provided,
        a default template is used.
    template_path : str | None, optional
        Optional path to a custom template file. If provided, this overrides
        the default template or any template specified by name.
    output : str | None, optional
        Optional path to write rendered output to. If not provided, output is
        printed to stdout.
    pretty : bool, optional
        Whether to pretty-print the rendered output. Default is ``True``.
    quiet : bool, optional
        Whether to suppress non-error output. Default is ``False``.
    resolve_render_template_fn : Any
        Function to resolve the rendering template. Should accept *template*
        and *template_path* and return a tuple of (template_key,
        file_override).
    summary_module : Any
        Module containing the summary.collect_table_specs function.
    render_tables_fn : Any
        Function to render tables. Should accept specs, template, and
        template_path.
    emit_render_output_fn : Any
        Function to emit the rendered output. Should accept rendered_chunks,
        output_path, pretty, quiet, and schema_count.
    print_fn : Any
        Function to print messages. Should accept a message and a file (e.g.,
        stderr).
    stderr : Any
        The stderr stream to use for printing error messages.

    Returns
    -------
    int
        Exit code. Returns 0 on success, 1 on failure.
    """
    template_key, file_override = resolve_render_template_fn(template, template_path)
    specs = summary_module.collect_table_specs(config, spec)
    if table:
        specs = [
            spec
            for spec in specs
            if str(spec.get('table')) == table or str(spec.get('name', '')) == table
        ]

    if not specs:
        target_desc = table or 'table_schemas'
        print_fn(
            'No table schemas found for '
            f'{target_desc}. Provide --spec or a pipeline --config with '
            'table_schemas.',
            file=stderr,
        )
        return 1

    rendered_chunks = render_tables_fn(
        specs,
        template=template_key,
        template_path=file_override,
    )
    return emit_render_output_fn(
        rendered_chunks,
        output_path=output,
        pretty=pretty,
        quiet=quiet,
        schema_count=len(specs),
    )
