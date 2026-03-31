"""
:mod:`etlplus.cli._handler_render` module.

SQL render helpers for the CLI facade.
"""

from __future__ import annotations

from typing import Any

from ..utils._types import TemplateKey
from . import _handler_common as _common_impl
from . import _summary

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
    template: TemplateKey | None = None,
    template_path: str | None = None,
    output: str | None = None,
    pretty: bool = True,
    quiet: bool = False,
    render_tables_fn: Any,
    print_fn: Any,
    stderr: Any,
) -> int:
    """
    Render SQL DDL statements from table schema specs.

    Parameters
    ----------
    config : str | None, optional
        Optional path to a config file containing table schema specs.
        Default is ``None``.
    spec : str | None, optional
        Optional path to a single table schema spec file. If provided, this
        takes precedence over any specs defined in a config file. Default is
        ``None``.
    table : str | None, optional
        Optional table name for filtering specs. Matches against the 'table' or
        'name' field in specs. If provided, only specs matching this table name
        will be rendered. Default is ``None``.
    template : TemplateKey | None, optional
        Optional key of template to use for rendering. If not provided, a
        default template will be used. Default is ``None``.
    template_path : str | None, optional
        Optional path to a custom template file. If provided, this will
        override the template specified by the *template* parameter. Default is
        ``None``.
    output : str | None, optional
        Optional path to write the rendered output to. If not provided, output
        will be printed to stdout. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print the rendered output. Default is ``True``.
    quiet : bool, optional
        Whether to suppress output. Default is ``False``.
    render_tables_fn : Any
        Callable to render tables with the specified template. Should, at
        minimum, accept the following parameters:
            - specs: list of table schema specifications
            - template: template key
            - template_path: path to a custom template file
    print_fn : Any
        Callable to print output (e.g., :func:`print`). Should accept the
        following parameters:
            - message: str, the message to print
            - file: optional, the file to print to (e.g., :func:`sys.stdout`)
    stderr : Any
        Stream to write error messages to (e.g., :func:`sys.stderr`).

    Returns
    -------
    int
        Exit code ``0`` on success; non-zero on error.
    """
    template_key, file_override = _common_impl.resolve_render_template(
        template,
        template_path,
    )
    specs = _summary.collect_table_specs(config, spec)
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

    return _common_impl.emit_render_output(
        render_tables_fn(
            specs,
            template=template_key,
            template_path=file_override,
        ),
        output_path=output,
        pretty=pretty,
        quiet=quiet,
        schema_count=len(specs),
        print_fn=print_fn,
    )
