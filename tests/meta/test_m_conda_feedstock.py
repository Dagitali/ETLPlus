"""
:mod:`tests.meta.test_m_conda_feedstock` module.

Guardrails for the support-gate-validated conda-forge feedstock recipe.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

from tests.meta.pytest_meta_support import REPO_ROOT
from tests.meta.pytest_meta_support import text_snippet_case_id
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
CONDA_WORKFLOW_PATH = REPO_ROOT / '.github/workflows/conda-recipe.yml'

CONDA_STATUS_TEXT_PATHS = (
    CONDA_README_PATH,
    CONDA_PREP_PATH,
    CONDA_SUBMISSION_PATH,
    CONDA_WORKFLOW_PATH,
)

STALE_PENDING_SUPPORT_PHRASES = (
    'before declaring conda-forge supported',
    'before support',
    'conda-forge remains a prepared but unsupported install channel',
    'cross-platform builds are still undecided',
    'not yet a supported',
    'repeat the recipe build/test',
)
CONDA_PLATFORM_ISOLATION_SNIPPETS = (
    'isolate macOS or Windows runs',
    'pins `micromamba` and `conda-build=25`',
    '`platform_scope: macos`',
    '`platform_scope: windows`',
    '`platform_scope: all`',
)
CONDA_STAGED_RECIPE_SUBMISSION_SNIPPETS = (
    (CONDA_README_PATH, 'STAGED-RECIPES-SUBMISSION.md'),
    (CONDA_PREP_PATH, 'STAGED-RECIPES-SUBMISSION.md'),
    (CONDA_SUBMISSION_PATH, '`conda-forge/staged-recipes`'),
    (CONDA_SUBMISSION_PATH, 'recipes/etlplus/meta.yaml'),
    (CONDA_SUBMISSION_PATH, 'released-version-without-leading-v'),
)
CONDA_TEMPLATE_SOURCE_SNIPPETS = (
    (CONDA_README_PATH, 'meta.yaml.j2'),
    (CONDA_PREP_PATH, 'meta.yaml.j2'),
    (CONDA_SUBMISSION_PATH, 'meta.yaml.j2'),
    (CONDA_README_PATH, 'tools/render_conda_recipe.py'),
    (CONDA_PREP_PATH, 'tools/render_conda_recipe.py'),
    (CONDA_SUBMISSION_PATH, 'tools/render_conda_recipe.py'),
)
CONDA_NAME_MAPPING_SNIPPETS = (
    (CONDA_RECIPE_PATH, 'msgpack-python >=1.0.8'),
    (CONDA_PREP_PATH, '`msgpack>=1.0.8` | `msgpack-python >=1.0.8`'),
    (CONDA_PREP_PATH, '`PyYAML>=6.0.3` | `pyyaml >=6.0.3`'),
    (CONDA_PREP_PATH, '`SQLAlchemy>=2.0.45` | `sqlalchemy >=2.0.45`'),
)
CONDA_RECIPE_PLACEHOLDERS = (
    '<release-version>',
    '<sdist-sha256>',
    '<maintainer-github-handle>',
)
CONDA_RECIPE_CLI_SNIPPETS = (
    'etlplus = etlplus.cli:main',
    'etlplus --version',
    'etlplus --help',
    'etlplus check --help',
)
CONDA_VALIDATED_STATUS_PATHS = (
    CONDA_README_PATH,
    CONDA_PREP_PATH,
)
CONDA_VALIDATED_STATUS_PATH_IDS = tuple(
    path.name for path in CONDA_VALIDATED_STATUS_PATHS
)
CONDA_VALIDATED_STATUS_SNIPPETS = (
    'tagged pypi sdist',
    'linux, macos, and windows',
    'feedstock',
    'accept',
)
CONDA_WORKFLOW_REQUIRED_SNIPPETS = (
    'workflow_dispatch:',
    'default: linux',
    'source_mode:',
    'tagged-sdist',
    'release_version',
    'sdist_sha256',
    'tagged-sdist validation requires release_version and sdist_sha256',
    'ubuntu-latest',
    'macos-latest',
    'windows-latest',
    '- macos',
    '- windows',
    "inputs.platform_scope == 'all'",
    "inputs.platform_scope == 'macos'",
    "inputs.platform_scope == 'windows'",
    "MICROMAMBA_VERSION: '2.0.5-0'",
    'micromamba-version: ${{ env.MICROMAMBA_VERSION }}',
    'conda-build=25',
    'Diagnose conda tooling',
    'micromamba --version',
    'conda-build --version',
    'conda info',
    'tools/render_conda_recipe.py',
    'conda-build "${RUNNER_TEMP}/etlplus-conda-recipe"',
)


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


@pytest.mark.parametrize('snippet', CONDA_PLATFORM_ISOLATION_SNIPPETS)
def test_conda_docs_reference_platform_isolation_options(snippet: str) -> None:
    """Test conda docs explain platform-specific validation dispatch options."""
    prep_text = CONDA_PREP_PATH.read_text(encoding='utf-8')

    assert snippet in prep_text


@pytest.mark.parametrize(
    ('path', 'snippet'),
    CONDA_STAGED_RECIPE_SUBMISSION_SNIPPETS,
    ids=[
        text_snippet_case_id(case) for case in CONDA_STAGED_RECIPE_SUBMISSION_SNIPPETS
    ],
)
def test_conda_docs_reference_staged_recipes_submission_path(
    path: Path,
    snippet: str,
) -> None:
    """Test conda docs point maintainers at the staged-recipes submission path."""
    text = path.read_text(encoding='utf-8')

    assert snippet in text


@pytest.mark.parametrize(
    ('path', 'snippet'),
    CONDA_TEMPLATE_SOURCE_SNIPPETS,
    ids=[text_snippet_case_id(case) for case in CONDA_TEMPLATE_SOURCE_SNIPPETS],
)
def test_conda_docs_reference_template_recipe_source(
    path: Path,
    snippet: str,
) -> None:
    """Test conda docs point maintainers at the Jinja recipe source."""
    text = path.read_text(encoding='utf-8')

    assert snippet in text


@pytest.mark.parametrize(
    ('path', 'snippet'),
    CONDA_NAME_MAPPING_SNIPPETS,
    ids=[text_snippet_case_id(case) for case in CONDA_NAME_MAPPING_SNIPPETS],
)
def test_conda_recipe_documents_expected_name_mappings(
    path: Path,
    snippet: str,
) -> None:
    """Test the known PyPI-to-conda package name differences are explicit."""
    text = path.read_text(encoding='utf-8')

    assert snippet in text


@pytest.mark.parametrize('placeholder', CONDA_RECIPE_PLACEHOLDERS)
def test_conda_recipe_keeps_feedstock_placeholders_explicit(
    placeholder: str,
) -> None:
    """Test release-specific feedstock values remain obvious placeholders."""
    recipe_text = CONDA_RECIPE_PATH.read_text(encoding='utf-8')

    assert placeholder in recipe_text


@pytest.mark.parametrize('snippet', CONDA_RECIPE_CLI_SNIPPETS)
def test_conda_recipe_preserves_cli_entrypoint_and_smoke_commands(
    snippet: str,
) -> None:
    """Test the candidate feedstock recipe exposes and verifies the ETLPlus CLI."""
    recipe_text = CONDA_RECIPE_PATH.read_text(encoding='utf-8')

    assert snippet in recipe_text


def test_conda_recipe_render_helper_replaces_release_placeholders(
    tmp_path: Path,
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
    tmp_path: Path,
) -> None:
    """Test tagged-sdist recipes require a real SHA256-looking value."""
    output_path = tmp_path / 'meta.yaml'

    with pytest.raises(ValueError, match='64-character hexadecimal SHA256'):
        render_recipe(
            template_path=CONDA_RECIPE_PATH,
            output_path=output_path,
            version='1.2.3',
            sha256='not-a-sha',
            maintainer='dagitali-maintainer',
        )


def test_conda_recipe_render_helper_supports_local_source_path(
    tmp_path: Path,
) -> None:
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


@pytest.mark.parametrize('snippet', CONDA_WORKFLOW_REQUIRED_SNIPPETS)
def test_conda_recipe_validation_workflow_is_manual_linux_first(
    snippet: str,
) -> None:
    """Test conda recipe CI remains manual and Linux-first by default."""
    workflow_text = CONDA_WORKFLOW_PATH.read_text(encoding='utf-8')

    assert snippet in workflow_text


def test_conda_status_docs_do_not_regress_to_pending_support_gate() -> None:
    """Test conda docs no longer describe completed validation as pending."""
    stale_hits: list[str] = []

    for path in CONDA_STATUS_TEXT_PATHS:
        text = path.read_text(encoding='utf-8').lower()
        stale_hits.extend(
            f'{path.relative_to(REPO_ROOT)}: {phrase}'
            for phrase in STALE_PENDING_SUPPORT_PHRASES
            if phrase in text
        )

    assert stale_hits == []


@pytest.mark.parametrize(
    'path',
    CONDA_VALIDATED_STATUS_PATHS,
    ids=CONDA_VALIDATED_STATUS_PATH_IDS,
)
def test_conda_status_docs_record_validated_but_unpublished_state(
    path: Path,
) -> None:
    """Test conda docs record the completed gate and publication handoff."""
    text = path.read_text(encoding='utf-8').lower()

    for snippet in CONDA_VALIDATED_STATUS_SNIPPETS:
        assert snippet in text
    assert 'publication' in text or 'published' in text


def test_conda_submission_docs_preserve_base_recipe_scope() -> None:
    """Test staged-recipes docs preserve the base-only feedstock scope."""
    submission_text = CONDA_SUBMISSION_PATH.read_text(encoding='utf-8')

    assert 'broad base PyPI runtime contract' in submission_text
    assert 'Do not add optional extras to the first recipe' in submission_text
    assert 'Preserve the dependency mappings documented in `FEEDSTOCK-PREP.md`' in (
        submission_text
    )
