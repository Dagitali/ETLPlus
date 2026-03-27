"""
:mod:`etlplus.cli._handlers.render` module.

Render-command helpers and handler.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from ... import Config
from ...database import load_table_spec
from ...database import render_tables
from ...utils.types import TemplateKey

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'render_handler',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _collect_table_specs(
    config_path: str | None,
    spec_path: str | None,
) -> list[dict[str, Any]]:
    """Load table schemas from a pipeline config and/or standalone spec."""
    specs: list[dict[str, Any]] = []

    if spec_path:
        specs.append(dict(load_table_spec(Path(spec_path))))

    if config_path:
        cfg = Config.from_yaml(config_path, substitute=True)
        specs.extend(getattr(cfg, 'table_schemas', []))

    return specs


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
) -> int:
    """
    Render SQL DDL statements from table schema specs.

    Parameters
    ----------
    config : str | None, optional
        Path to a pipeline YAML config file containing table schemas. Optional
        if *spec* is provided.
    spec : str | None, optional
        Path to a standalone table schema spec file. Optional if *config* is
        provided.
    table : str | None, optional
        Optional name of a specific table to render (matches against both the
        ``table`` and ``name`` fields of table specs). If not provided, all tables
        from the config and/or spec will be rendered.
    template : TemplateKey | None, optional
        Key of a built-in template to use for rendering. Ignored if
        *template_path* is provided. If neither *template* nor *template_path*
        is provided, the ``ddl`` built-in template will be used by default.
    template_path : str | None, optional
        Path to a custom template file to use for rendering. If provided, this
        will take precedence over the *template* argument.
    output : str | None, optional
        Optional output path to write rendered SQL to. If not provided or if
        set to "-", rendered SQL will be printed to stdout.
    pretty : bool, optional
        Whether to pretty-print the rendered SQL (e.g. by stripping trailing
        whitespace and ensuring a single trailing newline). Defaults to
        ``True``.
    quiet : bool, optional
        Whether to suppress informational messages about the rendering process.
        Defaults to ``False``.

    Returns
    -------
    int
        Exit code (0 if rendering succeeded, non-zero if any errors occurred).
    """
    template_value: TemplateKey = template or 'ddl'
    file_override = template_path
    template_key: TemplateKey | None = template_value
    if template_path is None:
        candidate_path = Path(template_value)
        if candidate_path.exists():
            file_override = str(candidate_path)
            template_key = None

    specs = _collect_table_specs(config, spec)
    if table:
        specs = [
            table_spec
            for table_spec in specs
            if str(table_spec.get('table')) == table
            or str(table_spec.get('name', '')) == table
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

    rendered_chunks = render_tables(
        specs,
        template=template_key,
        template_path=file_override,
    )
    sql_text = '\n'.join(chunk.rstrip() for chunk in rendered_chunks).rstrip() + '\n'
    rendered_output = sql_text if pretty else sql_text.rstrip('\n')

    if output and output != '-':
        Path(output).write_text(rendered_output, encoding='utf-8')
        if not quiet:
            print(f'Rendered {len(specs)} schema(s) to {output}')
        return 0

    print(rendered_output)
    return 0
