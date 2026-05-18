"""
:mod:`tests.meta.test_m_conda_feedstock` module.

Guardrails for the draft conda-forge feedstock preparation recipe.
"""

from __future__ import annotations

import re
import tomllib

from tests.meta.pytest_meta_support import REPO_ROOT
from tools.render_conda_recipe import render_recipe

# SECTION: HELPERS ========================================================== #


_CONDA_NAME_MAP = {
    'msgpack': 'msgpack-python',
    'pyyaml': 'pyyaml',
    'sqlalchemy': 'sqlalchemy',
}

PYPROJECT_PATH = REPO_ROOT / 'pyproject.toml'
CONDA_RECIPE_PATH = REPO_ROOT / 'packaging/conda/meta.yaml.j2'
CONDA_README_PATH = REPO_ROOT / 'packaging/conda/README.md'
CONDA_SPIKE_PATH = REPO_ROOT / 'CONDA-FORGE-SPIKE.md'


def _canonical_requirement_name(requirement: str) -> str:
    """Return the normalized package name from one requirement string."""
    match = re.match(r'\s*([A-Za-z0-9_.-]+)', requirement)
    if match is None:
        msg = f'Could not parse requirement name from {requirement!r}'
        raise AssertionError(msg)
    return match.group(1).lower().replace('_', '-')


def _conda_run_requirements(recipe_text: str) -> set[str]:
    """Return normalized run dependency names from the draft conda recipe."""
    in_run_section = False
    names: set[str] = set()

    for line in recipe_text.splitlines():
        if line == '  run:':
            in_run_section = True
            continue
        if in_run_section and line and not line.startswith('    '):
            break
        if in_run_section and line.startswith('    - '):
            names.add(_canonical_requirement_name(line.removeprefix('    - ')))

    return names


# SECTION: TESTS ============================================================ #


def test_conda_recipe_tracks_base_pyproject_dependencies() -> None:
    """
    Test that the draft conda recipe includes the base runtime dependency set.
    """
    pyproject = tomllib.loads(PYPROJECT_PATH.read_text(encoding='utf-8'))
    recipe_text = CONDA_RECIPE_PATH.read_text(encoding='utf-8')

    pyproject_names = {
        _CONDA_NAME_MAP.get(name, name)
        for requirement in pyproject['project']['dependencies']
        for name in [_canonical_requirement_name(requirement)]
    }

    assert pyproject_names <= _conda_run_requirements(recipe_text)


def test_conda_recipe_documents_expected_name_mappings() -> None:
    """Test the known PyPI-to-conda package name differences are explicit."""
    recipe_text = CONDA_RECIPE_PATH.read_text(encoding='utf-8')
    readme_text = CONDA_README_PATH.read_text(encoding='utf-8')

    assert 'msgpack-python >=1.0.8' in recipe_text
    assert '`msgpack` | `msgpack-python`' in readme_text
    assert '`PyYAML` | `pyyaml`' in readme_text
    assert '`SQLAlchemy` | `sqlalchemy`' in readme_text


def test_conda_recipe_preserves_cli_entrypoint_and_smoke_commands() -> None:
    """Test the draft feedstock recipe exposes and verifies the ETLPlus CLI."""
    recipe_text = CONDA_RECIPE_PATH.read_text(encoding='utf-8')

    assert 'etlplus = etlplus.cli:main' in recipe_text
    assert 'etlplus --version' in recipe_text
    assert 'etlplus --help' in recipe_text
    assert 'etlplus check --help' in recipe_text


def test_conda_recipe_keeps_feedstock_placeholders_explicit() -> None:
    """Test release-specific feedstock values remain obvious placeholders."""
    recipe_text = CONDA_RECIPE_PATH.read_text(encoding='utf-8')

    assert '<release-version>' in recipe_text
    assert '<sdist-sha256>' in recipe_text
    assert '<maintainer-github-handle>' in recipe_text


def test_conda_recipe_render_helper_replaces_release_placeholders(
    tmp_path,
) -> None:
    """Test the render helper produces a concrete recipe for build tools."""
    output_path = tmp_path / 'meta.yaml'

    render_recipe(
        template_path=CONDA_RECIPE_PATH,
        output_path=output_path,
        version='1.2.3',
        sha256='a' * 64,
        maintainer='dagitali-maintainer',
    )

    rendered = output_path.read_text(encoding='utf-8')
    assert '<release-version>' not in rendered
    assert '<sdist-sha256>' not in rendered
    assert '<maintainer-github-handle>' not in rendered
    assert '{% set version = "1.2.3" %}' in rendered
    assert 'sha256: ' + ('a' * 64) in rendered
    assert '    - dagitali-maintainer' in rendered


def test_conda_recipe_render_helper_supports_local_source_path(tmp_path) -> None:
    """Test local validation builds can render a path-based source recipe."""
    output_path = tmp_path / 'meta.yaml'

    render_recipe(
        template_path=CONDA_RECIPE_PATH,
        output_path=output_path,
        version='0.0.0',
        sha256='0' * 64,
        maintainer='dagitali-maintainer',
        source_path=REPO_ROOT,
    )

    rendered = output_path.read_text(encoding='utf-8')
    assert f'  path: {REPO_ROOT.resolve()}' in rendered
    assert '  url: https://pypi.org/' not in rendered
    assert '  sha256: ' not in rendered


def test_conda_docs_reference_template_recipe_source() -> None:
    """Test conda docs point maintainers at the Jinja recipe source."""
    readme_text = CONDA_README_PATH.read_text(encoding='utf-8')
    spike_text = CONDA_SPIKE_PATH.read_text(encoding='utf-8')

    assert 'meta.yaml.j2' in readme_text
    assert 'meta.yaml.j2' in spike_text
    assert 'tools/render_conda_recipe.py' in readme_text
    assert 'tools/render_conda_recipe.py' in spike_text
