"""
:mod:`etlplus.cli._handlers.render` module.

SQL render helpers for the CLI facade.
"""

from __future__ import annotations

import sys
from pathlib import Path

from ...database import render_tables
from ...utils._types import TemplateKey
from . import _payload
from . import _summary

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'render_handler',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _emit_render_output(
    rendered_chunks: list[str],
    *,
    output_path: str | None,
    pretty: bool,
    quiet: bool,
    schema_count: int,
) -> int:
    """Write rendered SQL to a file or STDOUT."""
    sql_text = '\n'.join(chunk.rstrip() for chunk in rendered_chunks).rstrip() + '\n'
    rendered_output = sql_text if pretty else sql_text.rstrip('\n')
    if output_path and output_path != '-':
        Path(output_path).write_text(rendered_output, encoding='utf-8')
        if not quiet:
            print(f'Rendered {schema_count} schema(s) to {output_path}')
        return 0

    print(rendered_output, end='')
    return 0


# SECTION: FUNCTIONS ======================================================== #


def render_handler(
    *,
    config: str | None = None,
    spec: str | None = None,
    table: str | None = None,
    template: TemplateKey | None = None,
    template_path: str | None = None,
    output: str | None = None,
    quiet: bool = False,
    pretty: bool = True,
) -> int:
    """
    Render SQL DDL from table schema specs.

    Parameters
    ----------
    config : str | None, optional
        Path to a YAML/JSON config file containing table schema specs.
        Default is ``None``.
    spec : str | None, optional
        Path to a single table schema spec file. If provided, this takes
        precedence over any specs defined in *config*. Default is ``None``.
    table : str | None, optional
        Optional table name filter. Matches the ``table`` or ``name`` field in
        each spec. Default is ``None``.
    template : TemplateKey | None, optional
        Optional built-in template key. Default is ``None``.
    template_path : str | None, optional
        Optional path to a custom template file. Overrides *template* when
        provided. Default is ``None``.
    output : str | None, optional
        Optional path to write rendered SQL. Default is ``None``.
    quiet : bool, optional
        Whether to suppress status output. Default is ``False``.
    pretty : bool, optional
        Whether to pretty-print rendered output. Default is ``True``.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    template_key, file_override = _payload.resolve_render_template(
        template,
        template_path,
    )
    specs = _summary.collect_table_specs(config, spec)
    if table:
        specs = [
            spec_item
            for spec_item in specs
            if str(spec_item.get('table')) == table
            or str(spec_item.get('name', '')) == table
        ]

    if not specs:
        target_desc = table or 'table_schemas'
        print(
            'No table schemas found for '
            f'{target_desc}. Provide --spec or a pipeline --config with '
            'table_schemas.',
            file=sys.stderr,
        )
        return 1

    return _emit_render_output(
        render_tables(
            specs,
            template=template_key,
            template_path=file_override,
        ),
        output_path=output,
        pretty=pretty,
        quiet=quiet,
        schema_count=len(specs),
    )
