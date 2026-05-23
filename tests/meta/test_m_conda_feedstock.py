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
CONDA_TEXT_SNIPPET_CASES = tuple(
    pytest.param(path, snippet, id=f'{path.name}:{snippet}')
    for path, snippet in (
        (CONDA_PREP_PATH, 'isolate macOS or Windows runs'),
        (CONDA_PREP_PATH, 'pins `micromamba` and `conda-build=25`'),
        (CONDA_PREP_PATH, '`platform_scope: macos`'),
        (CONDA_PREP_PATH, '`platform_scope: windows`'),
        (CONDA_PREP_PATH, '`platform_scope: all`'),
        (CONDA_README_PATH, 'STAGED-RECIPES-SUBMISSION.md'),
        (CONDA_PREP_PATH, 'STAGED-RECIPES-SUBMISSION.md'),
        (CONDA_SUBMISSION_PATH, '`conda-forge/staged-recipes`'),
        (CONDA_SUBMISSION_PATH, 'recipes/etlplus/meta.yaml'),
        (CONDA_SUBMISSION_PATH, 'released-version-without-leading-v'),
        (CONDA_README_PATH, 'meta.yaml.j2'),
        (CONDA_PREP_PATH, 'meta.yaml.j2'),
        (CONDA_SUBMISSION_PATH, 'meta.yaml.j2'),
        (CONDA_README_PATH, 'tools/render_conda_recipe.py'),
        (CONDA_PREP_PATH, 'tools/render_conda_recipe.py'),
        (CONDA_SUBMISSION_PATH, 'tools/render_conda_recipe.py'),
        (CONDA_RECIPE_PATH, 'msgpack-python >=1.0.8'),
        (CONDA_PREP_PATH, '`msgpack>=1.0.8` | `msgpack-python >=1.0.8`'),
        (CONDA_PREP_PATH, '`PyYAML>=6.0.3` | `pyyaml >=6.0.3`'),
        (CONDA_PREP_PATH, '`SQLAlchemy>=2.0.45` | `sqlalchemy >=2.0.45`'),
        (CONDA_RECIPE_PATH, '<release-version>'),
        (CONDA_RECIPE_PATH, '<sdist-sha256>'),
        (CONDA_RECIPE_PATH, '<maintainer-github-handle>'),
        (CONDA_RECIPE_PATH, 'etlplus = etlplus.cli:main'),
        (CONDA_RECIPE_PATH, 'etlplus --version'),
        (CONDA_RECIPE_PATH, 'etlplus --help'),
        (CONDA_RECIPE_PATH, 'etlplus check --help'),
        (CONDA_WORKFLOW_PATH, 'workflow_dispatch:'),
        (CONDA_WORKFLOW_PATH, 'default: linux'),
        (CONDA_WORKFLOW_PATH, 'source_mode:'),
        (CONDA_WORKFLOW_PATH, 'tagged-sdist'),
        (CONDA_WORKFLOW_PATH, 'release_version'),
        (CONDA_WORKFLOW_PATH, 'sdist_sha256'),
        (
            CONDA_WORKFLOW_PATH,
            'tagged-sdist validation requires release_version and sdist_sha256',
        ),
        (CONDA_WORKFLOW_PATH, 'ubuntu-latest'),
        (CONDA_WORKFLOW_PATH, 'macos-latest'),
        (CONDA_WORKFLOW_PATH, 'windows-latest'),
        (CONDA_WORKFLOW_PATH, '- macos'),
        (CONDA_WORKFLOW_PATH, '- windows'),
        (CONDA_WORKFLOW_PATH, "inputs.platform_scope == 'all'"),
        (CONDA_WORKFLOW_PATH, "inputs.platform_scope == 'macos'"),
        (CONDA_WORKFLOW_PATH, "inputs.platform_scope == 'windows'"),
        (CONDA_WORKFLOW_PATH, "MICROMAMBA_VERSION: '2.0.5-0'"),
        (CONDA_WORKFLOW_PATH, 'micromamba-version: ${{ env.MICROMAMBA_VERSION }}'),
        (CONDA_WORKFLOW_PATH, 'conda-build=25'),
        (CONDA_WORKFLOW_PATH, 'Diagnose conda tooling'),
        (CONDA_WORKFLOW_PATH, 'micromamba --version'),
        (CONDA_WORKFLOW_PATH, 'conda-build --version'),
        (CONDA_WORKFLOW_PATH, 'conda info'),
        (CONDA_WORKFLOW_PATH, 'tools/render_conda_recipe.py'),
        (CONDA_WORKFLOW_PATH, 'conda-build "${RUNNER_TEMP}/etlplus-conda-recipe"'),
        (CONDA_SUBMISSION_PATH, 'broad base PyPI runtime contract'),
        (CONDA_SUBMISSION_PATH, 'Do not add optional extras to the first recipe'),
        (
            CONDA_SUBMISSION_PATH,
            'Preserve the dependency mappings documented in `FEEDSTOCK-PREP.md`',
        ),
    )
)
CONDA_VALIDATED_STATUS_PATHS = tuple(
    pytest.param(path, id=path.name) for path in (CONDA_README_PATH, CONDA_PREP_PATH)
)
CONDA_VALIDATED_STATUS_SNIPPETS = (
    'tagged pypi sdist',
    'linux, macos, and windows',
    'feedstock',
    'accept',
)


def _canonical_requirement_name(requirement: str) -> str:
    """Return the normalized package name from one requirement string."""
    match = re.match(r'\s*([A-Za-z0-9_.-]+)', requirement)
    if match is None:
        msg = f'Could not parse requirement name from {requirement!r}'
        raise AssertionError(msg)
    return match.group(1).lower().replace('_', '-')


def _conda_run_requirement_specs(recipe_text: str) -> list[str]:
    """Return raw run dependency specs from the candidate conda recipe."""
    in_run_section = False
    requirements: list[str] = []

    for line in recipe_text.splitlines():
        if line == '  run:':
            in_run_section = True
            continue
        if in_run_section and line and not line.startswith('    '):
            break
        if in_run_section and line.startswith('    - '):
            requirements.append(line.removeprefix('    - '))

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


class TestCondaFeedstockGuardrails:
    """Meta guardrails for the support-gate-validated conda feedstock recipe."""

    @pytest.mark.parametrize(
        ('path', 'snippet'),
        CONDA_TEXT_SNIPPET_CASES,
    )
    def test_packaging_files_preserve_required_snippets(
        self,
        path: Path,
        snippet: str,
    ) -> None:
        """Test conda packaging docs, recipe, and workflow keep required snippets."""
        text = path.read_text(encoding='utf-8')

        assert snippet in text

    def test_recipe_render_helper_replaces_release_placeholders(
        self,
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

    def test_recipe_run_requirements_match_base_pyproject_dependencies(
        self,
    ) -> None:
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

        observed = {
            _normalized_requirement_line(requirement)
            for requirement in _conda_run_requirement_specs(recipe_text)
        }

        assert observed == expected

    def test_recipe_render_helper_rejects_invalid_release_sha256(
        self,
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

    def test_recipe_render_helper_supports_local_source_path(
        self,
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

    def test_recipe_tracks_base_pyproject_dependencies(self) -> None:
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

        recipe_names = {
            _canonical_requirement_name(requirement)
            for requirement in _conda_run_requirement_specs(recipe_text)
        }

        assert pyproject_names <= recipe_names

    def test_status_docs_do_not_regress_to_pending_support_gate(self) -> None:
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
    )
    def test_status_docs_record_validated_but_unpublished_state(
        self,
        path: Path,
    ) -> None:
        """Test conda docs record the completed gate and publication handoff."""
        text = path.read_text(encoding='utf-8').lower()

        for snippet in CONDA_VALIDATED_STATUS_SNIPPETS:
            assert snippet in text
        assert 'publication' in text or 'published' in text
