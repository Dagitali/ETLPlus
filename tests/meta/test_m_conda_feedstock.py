"""
:mod:`tests.meta.test_m_conda_feedstock` module.

Guardrails for the candidate conda-forge feedstock preparation recipe.
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
CONDA_PREP_PATH = REPO_ROOT / 'packaging/conda/FEEDSTOCK-PREP.md'
CONDA_SUBMISSION_PATH = REPO_ROOT / 'packaging/conda/STAGED-RECIPES-SUBMISSION.md'


def _canonical_requirement_name(requirement: str) -> str:
    """Return the normalized package name from one requirement string."""
    match = re.match(r'\s*([A-Za-z0-9_.-]+)', requirement)
    if match is None:
        msg = f'Could not parse requirement name from {requirement!r}'
        raise AssertionError(msg)
    return match.group(1).lower().replace('_', '-')


def _conda_run_requirements(recipe_text: str) -> set[str]:
    """Return normalized run dependency names from the candidate conda recipe."""
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


def _conda_run_requirement_lines(recipe_text: str) -> set[str]:
    """Return normalized run dependency lines from the candidate conda recipe."""
    in_run_section = False
    requirements: set[str] = set()

    for line in recipe_text.splitlines():
        if line == '  run:':
            in_run_section = True
            continue
        if in_run_section and line and not line.startswith('    '):
            break
        if in_run_section and line.startswith('    - '):
            requirements.add(
                _normalized_requirement_line(line.removeprefix('    - ')),
            )

    return requirements


def _normalized_requirement_line(requirement: str) -> str:
    """Return a normalized requirement string for recipe/pyproject comparison."""
    return re.sub(
        r'\s*(<=|>=|==|!=|~=|<|>)\s*',
        r'\1',
        requirement.strip().lower(),
    )


def _conda_runtime_requirement(requirement: str) -> str:
    """Return the expected conda-forge requirement for a PyPI dependency."""
    name = _canonical_requirement_name(requirement)
    conda_name = _CONDA_NAME_MAP.get(name, name)
    return _normalized_requirement_line(
        re.sub(
            r'^\s*[A-Za-z0-9_.-]+',
            conda_name,
            requirement,
            count=1,
        ),
    )


# SECTION: TESTS ============================================================ #


def test_conda_docs_reference_platform_isolation_options() -> None:
    """Test conda docs explain platform-specific validation dispatch options."""
    readme_text = CONDA_README_PATH.read_text(encoding='utf-8')
    prep_text = CONDA_PREP_PATH.read_text(encoding='utf-8')

    assert 'isolate macOS or Windows runs' in readme_text
    assert 'pins `micromamba` and `conda-build=25`' in readme_text
    assert '`platform_scope: macos`' in prep_text
    assert '`platform_scope: windows`' in prep_text
    assert '`platform_scope: all`' in prep_text


def test_conda_docs_reference_staged_recipes_submission_path() -> None:
    """Test conda docs point maintainers at the staged-recipes submission path."""
    readme_text = CONDA_README_PATH.read_text(encoding='utf-8')
    prep_text = CONDA_PREP_PATH.read_text(encoding='utf-8')
    submission_text = CONDA_SUBMISSION_PATH.read_text(encoding='utf-8')

    assert 'STAGED-RECIPES-SUBMISSION.md' in readme_text
    assert 'STAGED-RECIPES-SUBMISSION.md' in prep_text
    assert '`conda-forge/staged-recipes`' in submission_text
    assert 'recipes/etlplus/meta.yaml' in submission_text
    assert 'released-version-without-leading-v' in submission_text


def test_conda_docs_reference_template_recipe_source() -> None:
    """Test conda docs point maintainers at the Jinja recipe source."""
    readme_text = CONDA_README_PATH.read_text(encoding='utf-8')
    prep_text = CONDA_PREP_PATH.read_text(encoding='utf-8')
    submission_text = CONDA_SUBMISSION_PATH.read_text(encoding='utf-8')

    assert 'meta.yaml.j2' in readme_text
    assert 'meta.yaml.j2' in prep_text
    assert 'meta.yaml.j2' in submission_text
    assert 'tools/render_conda_recipe.py' in readme_text
    assert 'tools/render_conda_recipe.py' in prep_text
    assert 'tools/render_conda_recipe.py' in submission_text


def test_conda_submission_docs_preserve_base_recipe_scope() -> None:
    """Test staged-recipes docs preserve the base-only feedstock scope."""
    submission_text = CONDA_SUBMISSION_PATH.read_text(encoding='utf-8')

    assert 'broad base PyPI runtime contract' in submission_text
    assert 'Do not add optional extras to the first recipe' in submission_text
    assert '`msgpack` | `msgpack-python`' in submission_text
    assert '`PyYAML` | `pyyaml`' in submission_text
    assert '`SQLAlchemy` | `sqlalchemy`' in submission_text


def test_conda_recipe_documents_expected_name_mappings() -> None:
    """Test the known PyPI-to-conda package name differences are explicit."""
    recipe_text = CONDA_RECIPE_PATH.read_text(encoding='utf-8')
    readme_text = CONDA_README_PATH.read_text(encoding='utf-8')

    assert 'msgpack-python >=1.0.8' in recipe_text
    assert '`msgpack` | `msgpack-python`' in readme_text
    assert '`PyYAML` | `pyyaml`' in readme_text
    assert '`SQLAlchemy` | `sqlalchemy`' in readme_text


def test_conda_recipe_keeps_feedstock_placeholders_explicit() -> None:
    """Test release-specific feedstock values remain obvious placeholders."""
    recipe_text = CONDA_RECIPE_PATH.read_text(encoding='utf-8')

    assert '<release-version>' in recipe_text
    assert '<sdist-sha256>' in recipe_text
    assert '<maintainer-github-handle>' in recipe_text


def test_conda_recipe_preserves_cli_entrypoint_and_smoke_commands() -> None:
    """Test the candidate feedstock recipe exposes and verifies the ETLPlus CLI."""
    recipe_text = CONDA_RECIPE_PATH.read_text(encoding='utf-8')

    assert 'etlplus = etlplus.cli:main' in recipe_text
    assert 'etlplus --version' in recipe_text
    assert 'etlplus --help' in recipe_text
    assert 'etlplus check --help' in recipe_text


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


def test_conda_recipe_run_requirements_match_base_pyproject_dependencies() -> None:
    """
    Test that conda run requirements stay aligned with the broad PyPI base.
    """
    pyproject = tomllib.loads(PYPROJECT_PATH.read_text(encoding='utf-8'))
    recipe_text = CONDA_RECIPE_PATH.read_text(encoding='utf-8')

    python_requirement = f'python {pyproject["project"]["requires-python"]}'
    expected = {
        _normalized_requirement_line(python_requirement),
        *{
            _conda_runtime_requirement(requirement)
            for requirement in pyproject['project']['dependencies']
        },
    }

    assert _conda_run_requirement_lines(recipe_text) == expected


def test_conda_recipe_tracks_base_pyproject_dependencies() -> None:
    """
    Test that the candidate conda recipe includes the base runtime dependency set.
    """
    pyproject = tomllib.loads(PYPROJECT_PATH.read_text(encoding='utf-8'))
    recipe_text = CONDA_RECIPE_PATH.read_text(encoding='utf-8')

    pyproject_names = {
        _CONDA_NAME_MAP.get(name, name)
        for requirement in pyproject['project']['dependencies']
        for name in [_canonical_requirement_name(requirement)]
    }

    assert pyproject_names <= _conda_run_requirements(recipe_text)


def test_conda_recipe_render_helper_rejects_invalid_release_sha256(
    tmp_path,
) -> None:
    """Test tagged-sdist recipes require a real SHA256-looking value."""
    output_path = tmp_path / 'meta.yaml'

    try:
        render_recipe(
            template_path=CONDA_RECIPE_PATH,
            output_path=output_path,
            version='1.2.3',
            sha256='not-a-sha',
            maintainer='dagitali-maintainer',
        )
    except ValueError as exc:
        assert '64-character hexadecimal SHA256' in str(exc)
    else:
        raise AssertionError('Expected invalid release SHA256 to be rejected.')


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
    assert f'  path: "{REPO_ROOT.resolve()}"' in rendered
    assert '  url: https://pypi.org/' not in rendered
    assert '  sha256: ' not in rendered


def test_conda_recipe_validation_workflow_is_manual_linux_first() -> None:
    """Test conda recipe CI remains manual and Linux-first by default."""
    workflow_text = (REPO_ROOT / '.github/workflows/conda-recipe.yml').read_text(
        encoding='utf-8',
    )

    assert 'workflow_dispatch:' in workflow_text
    assert 'default: linux' in workflow_text
    assert 'source_mode:' in workflow_text
    assert 'tagged-sdist' in workflow_text
    assert 'release_version' in workflow_text
    assert 'sdist_sha256' in workflow_text
    assert (
        'tagged-sdist validation requires release_version and sdist_sha256'
        in workflow_text
    )
    assert 'ubuntu-latest' in workflow_text
    assert 'macos-latest' in workflow_text
    assert 'windows-latest' in workflow_text
    assert '- macos' in workflow_text
    assert '- windows' in workflow_text
    assert "inputs.platform_scope == 'all'" in workflow_text
    assert "inputs.platform_scope == 'macos'" in workflow_text
    assert "inputs.platform_scope == 'windows'" in workflow_text
    assert "MICROMAMBA_VERSION: '2.0.5-0'" in workflow_text
    assert 'micromamba-version: ${{ env.MICROMAMBA_VERSION }}' in workflow_text
    assert 'conda-build=25' in workflow_text
    assert 'Diagnose conda tooling' in workflow_text
    assert 'micromamba --version' in workflow_text
    assert 'conda-build --version' in workflow_text
    assert 'conda info' in workflow_text
    assert 'tools/render_conda_recipe.py' in workflow_text
    assert 'conda-build "${RUNNER_TEMP}/etlplus-conda-recipe"' in workflow_text
