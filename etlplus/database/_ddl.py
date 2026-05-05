"""
:mod:`etlplus.database._ddl` module.

DDL rendering utilities for pipeline table schemas.

Exposes helpers to load YAML/JSON table specs and render them into SQL via
Jinja templates. Mirrors the behavior of ``tools/render_ddl.py`` so the CLI
can emit DDLs without shelling out to that script.
"""

from __future__ import annotations

import importlib.resources
import os
from collections.abc import Iterable
from collections.abc import Mapping
from pathlib import Path
from typing import Final

from ..file import File
from ..file.jinja2 import Jinja2File
from ..utils._types import StrAnyMap
from ..utils._types import StrPath
from ..utils._types import TemplateKey

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'TEMPLATES',
    'load_table_spec',
    'render_table_sql',
    'render_tables',
    'render_tables_to_string',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_SUPPORTED_SPEC_SUFFIXES: Final[frozenset[str]] = frozenset(
    {
        '.json',
        '.yml',
        '.yaml',
    },
)

_JINJA2_HANDLER: Final[Jinja2File] = Jinja2File()


# SECTION: CONSTANTS ======================================================== #


TEMPLATES: Final[dict[TemplateKey, str]] = {
    'ddl': 'ddl.sql.j2',
    'view': 'view.sql.j2',
}


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _load_template_text(
    filename: str,
) -> str:
    """
    Return the bundled template text.

    Parameters
    ----------
    filename : str
        Template filename located inside the package data folder.

    Returns
    -------
    str
        Raw template contents.

    Raises
    ------
    FileNotFoundError
        If the template file cannot be located in package data.
    """
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
            f'Could not load template {filename} from etlplus.templates package data.',
        ) from exc


def _resolve_template(
    *,
    template_key: TemplateKey | None,
    template_path: StrPath | None,
) -> str:
    """
    Return template text for rendering.

    Parameters
    ----------
    template_key : TemplateKey | None
        Named template key bundled with the package.
    template_path : StrPath | None
        Explicit template file override.

    Returns
    -------
    str
        Resolved template source code.

    Raises
    ------
    FileNotFoundError
        If the provided template path does not exist.
    ValueError
        If the template key is unknown.
    """
    override_path = template_path or os.environ.get('TEMPLATE_NAME')
    if override_path:
        path = Path(override_path)
        if not path.exists():
            raise FileNotFoundError(f'Template file not found: {path}')
        return _template_text_from_override_payload(_JINJA2_HANDLER.at(path).read())

    key: TemplateKey = template_key or 'ddl'
    if key not in TEMPLATES:
        choices = ', '.join(sorted(TEMPLATES))
        raise ValueError(
            f'Unknown template key "{key}". Choose from: {choices}',
        )

    return _load_template_text(TEMPLATES[key])


def _template_text_from_override_payload(
    payload: object,
) -> str:
    """Return template text from a file-handler override payload."""
    if (
        isinstance(payload, list)
        and payload
        and isinstance(payload[0], Mapping)
        and isinstance(payload[0].get('template'), str)
    ):
        return payload[0]['template']
    raise TypeError('JINJA2 template payload must include text')


# SECTION: FUNCTIONS ======================================================== #


def load_table_spec(
    path: StrPath,
) -> StrAnyMap:
    """
    Load a table specification from disk.

    Parameters
    ----------
    path : StrPath
        Path to the JSON or YAML specification file.

    Returns
    -------
    StrAnyMap
        Parsed table specification mapping.

    Raises
    ------
    ImportError
        If the file cannot be read due to missing dependencies.
    RuntimeError
        If the YAML dependency is missing for YAML specs.
    TypeError
        If the loaded spec is not a mapping.
    ValueError
        If the file suffix is not supported.
    """
    spec_path = Path(path)
    suffix = spec_path.suffix.lower()

    if suffix not in _SUPPORTED_SPEC_SUFFIXES:
        raise ValueError('Spec must be .json, .yml, or .yaml')

    try:
        spec = File(spec_path).read()
    except ImportError as e:
        if suffix in {'.yml', '.yaml'}:
            raise RuntimeError(
                'Missing dependency: pyyaml is required for YAML specs.',
            ) from e
        raise

    if not isinstance(spec, Mapping):
        raise TypeError('Table spec must be a mapping')

    return dict(spec)


def render_table_sql(
    spec: StrAnyMap,
    *,
    template: TemplateKey | None = 'ddl',
    template_path: StrPath | None = None,
) -> str:
    """
    Render a single table spec into SQL text.

    Parameters
    ----------
    spec : StrAnyMap
        Table specification mapping.
    template : TemplateKey | None, optional
        Template key to use (default: 'ddl').
    template_path : StrPath | None, optional
        Path to a custom template file (overrides *template*).

    Returns
    -------
    str
        Rendered SQL string.
    """
    template_source = _resolve_template(
        template_key=template,
        template_path=template_path,
    )
    rendered_sql = _JINJA2_HANDLER.render(
        template_source,
        {'spec': spec},
        strict_undefined=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    return rendered_sql.rstrip() + '\n'


def render_tables(
    specs: Iterable[StrAnyMap],
    *,
    template: TemplateKey | None = 'ddl',
    template_path: StrPath | None = None,
) -> list[str]:
    """
    Render multiple table specs into a list of SQL payloads.

    Parameters
    ----------
    specs : Iterable[StrAnyMap]
        Table specification mappings.
    template : TemplateKey | None, optional
        Template key to use (default: 'ddl').
    template_path : StrPath | None, optional
        Path to a custom template file (overrides *template*).

    Returns
    -------
    list[str]
        Rendered SQL strings for each table spec.
    """
    return [
        render_table_sql(spec, template=template, template_path=template_path)
        for spec in specs
    ]


def render_tables_to_string(
    spec_paths: Iterable[StrPath],
    *,
    template: TemplateKey | None = 'ddl',
    template_path: StrPath | None = None,
) -> str:
    """
    Render one or more specs and concatenate the SQL payloads.

    Parameters
    ----------
    spec_paths : Iterable[StrPath]
        Paths to table specification files.
    template : TemplateKey | None, optional
        Template key bundled with ETLPlus. Defaults to ``'ddl'``.
    template_path : StrPath | None, optional
        Custom Jinja template to override the bundled templates.

    Returns
    -------
    str
        Concatenated SQL payload suitable for writing to disk or STDOUT.
    """
    return ''.join(
        render_table_sql(
            load_table_spec(spec_path),
            template=template,
            template_path=template_path,
        )
        for spec_path in spec_paths
    )
