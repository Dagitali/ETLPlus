"""
:mod:`etlplus.ddl` module.

DDL rendering utilities for pipeline table schemas.

Exposes helpers to load YAML/JSON table specs and render them into SQL via
Jinja templates. Mirrors the behavior of ``tools/render_ddl.py`` so the CLI
can emit DDLs without shelling out to that script.
"""

from __future__ import annotations

import importlib.resources
import json
import os
from collections.abc import Iterable
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from jinja2 import DictLoader
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import StrictUndefined

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'TEMPLATES',
    'load_table_spec',
    'render_table_sql',
    'render_tables',
]


# SECTION: CONSTANTS ======================================================== #


TEMPLATES = {
    'ddl': 'ddl.sql.j2',
    'view': 'view.sql.j2',
}


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _build_env(
    *,
    template_key: str | None,
    template_path: str | None,
) -> Environment:
    """Return a Jinja2 environment using a built-in or file template."""
    file_override = template_path or os.environ.get('TEMPLATE_NAME')
    if file_override:
        path = Path(file_override)
        if not path.exists():
            raise FileNotFoundError(f'Template file not found: {path}')
        loader = FileSystemLoader(str(path.parent))
        env = Environment(
            loader=loader,
            undefined=StrictUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        env.globals['TEMPLATE_NAME'] = path.name
        return env

    key = (template_key or 'ddl').strip()
    if key not in TEMPLATES:
        choices = ', '.join(sorted(TEMPLATES))
        raise ValueError(
            f'Unknown template key "{key}". Choose from: {choices}',
        )

    # Load template from package data
    template_filename = TEMPLATES[key]
    template_source = _load_template_text(template_filename)

    env = Environment(
        loader=DictLoader({key: template_source}),
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.globals['TEMPLATE_NAME'] = key
    return env


def _load_template_text(filename: str) -> str:
    """Return the raw template text bundled with the package."""

    try:
        return (
            importlib.resources.files(
                'etlplus.templates',
            )
            .joinpath(filename)
            .read_text(encoding='utf-8')
        )
    except FileNotFoundError as exc:  # pragma: no cover - deployment guard
        raise FileNotFoundError(
            f'Could not load template {filename} '
            f'from etlplus.templates package data.',
        ) from exc


# SECTION: FUNCTIONS ======================================================== #


def load_table_spec(path: Path | str) -> dict[str, Any]:
    """Load a table spec from JSON or YAML."""

    spec_path = Path(path)
    text = spec_path.read_text(encoding='utf-8')
    suffix = spec_path.suffix.lower()

    if suffix == '.json':
        return json.loads(text)

    if suffix in {'.yml', '.yaml'}:
        try:
            import yaml  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                'Missing dependency: pyyaml is required for YAML specs.',
            ) from exc
        return yaml.safe_load(text)

    raise ValueError('Spec must be .json, .yml, or .yaml')


def render_table_sql(
    spec: Mapping[str, Any],
    *,
    template: str | None = 'ddl',
    template_path: str | None = None,
) -> str:
    """
    Render a single table spec into SQL text.

    Parameters
    ----------
    spec : Mapping[str, Any]
        Table specification mapping.
    template : str | None, optional
        Template key to use (default: 'ddl').
    template_path : str | None, optional
        Path to a custom template file (overrides ``template``).

    Returns
    -------
    str
        Rendered SQL string.

    Raises
    ------
    TypeError
        If the loaded template name is not a string.
    """
    env = _build_env(template_key=template, template_path=template_path)
    template_name = env.globals.get('TEMPLATE_NAME')
    if not isinstance(template_name, str):
        raise TypeError('TEMPLATE_NAME must be a string.')
    tmpl = env.get_template(template_name)
    return tmpl.render(spec=spec).rstrip() + '\n'


def render_tables(
    specs: Iterable[Mapping[str, Any]],
    *,
    template: str | None = 'ddl',
    template_path: str | None = None,
) -> list[str]:
    """
    Render multiple table specs into a list of SQL payloads.

    Parameters
    ----------
    specs : Iterable[Mapping[str, Any]]
        Table specification mappings.
    template : str | None, optional
        Template key to use (default: 'ddl').
    template_path : str | None, optional
        Path to a custom template file (overrides ``template``).

    Returns
    -------
    list[str]
        Rendered SQL strings for each table spec.
    """

    return [
        render_table_sql(spec, template=template, template_path=template_path)
        for spec in specs
    ]
