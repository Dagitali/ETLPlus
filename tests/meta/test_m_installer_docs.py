"""
:mod:`tests.meta.test_m_installer_docs` module.

Guardrails for supported installer documentation and release smoke coverage.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.meta.pytest_meta_support import REPO_ROOT

# SECTION: TYPES ============================================================ #


type InstallerContract = tuple[str, re.Pattern[str]]


# SECTION: CONSTANTS ======================================================== #


README_PATH = REPO_ROOT / 'README.md'
RELEASE_CHECKLIST_PATH = REPO_ROOT / 'RELEASE-CHECKLIST.md'
COMPATIBILITY_PATH = REPO_ROOT / 'docs/source/getting-started/compatibility.md'

CI_WORKFLOW_PATH = REPO_ROOT / '.github/workflows/ci.yml'
INSTALLER_SMOKE_ACTION_PATH = REPO_ROOT / '.github/actions/installer-smoke/action.yml'

CONDA_STATUS_DOC_PATHS = (
    README_PATH,
    COMPATIBILITY_PATH,
    RELEASE_CHECKLIST_PATH,
)

CROSS_PLATFORM_SMOKE_SNIPPETS = (
    'os: [macos-latest, windows-latest]',
    'etlplus --version',
    'etlplus --help',
    'etlplus check --help',
)
CONDA_STATUS_SNIPPETS = (
    'conda-forge',
    'tagged',
    'published',
)


INSTALLER_CONTRACTS = (
    pytest.param(
        (
            'pip install etlplus',
            re.compile(r'-m pip install \$\{\{ inputs\.artifact-wheel \}\}'),
        ),
        id='pip',
    ),
    pytest.param(
        (
            'pipx install etlplus',
            re.compile(r'-m pipx install \$\{\{ inputs\.artifact-wheel \}\}'),
        ),
        id='pipx',
    ),
    pytest.param(
        (
            'uv tool install etlplus',
            re.compile(r'uv tool install --force \$\{\{ inputs\.artifact-wheel \}\}'),
        ),
        id='uv',
    ),
)

CONDA_STATUS_CASES = tuple(
    pytest.param(path, snippet, id=f'{path.name}:{snippet}')
    for path in CONDA_STATUS_DOC_PATHS
    for snippet in CONDA_STATUS_SNIPPETS
)


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='installer_smoke_action_text', scope='module')
def installer_smoke_action_text_fixture() -> str:
    """Return the supported installer smoke action source."""
    return INSTALLER_SMOKE_ACTION_PATH.read_text(encoding='utf-8')


# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    ('path', 'snippet'),
    CONDA_STATUS_CASES,
)
def test_conda_status_is_documented_as_validated_but_unpublished(
    path: Path,
    snippet: str,
) -> None:
    """
    Test that conda packaging is documented as support-gate validated without
    claiming user-facing install availability before feedstock publication.
    """
    normalized_text = path.read_text(encoding='utf-8').lower()

    assert snippet in normalized_text


@pytest.mark.parametrize('snippet', CROSS_PLATFORM_SMOKE_SNIPPETS)
def test_cross_platform_smoke_checks_cli_help_surfaces(snippet: str) -> None:
    """Test macOS/Windows smoke coverage checks stable CLI help surfaces."""
    workflow_text = CI_WORKFLOW_PATH.read_text(encoding='utf-8')

    assert snippet in workflow_text


def test_installer_smoke_resolves_tool_installer_entrypoint_from_path(
    installer_smoke_action_text: str,
) -> None:
    """
    Test that tool-installer smoke checks do not assume a fixed app-bin path.
    """
    assert '$HOME/.local/bin/etlplus' not in installer_smoke_action_text
    assert 'etlplus_bin="$(command -v etlplus)"' in installer_smoke_action_text


@pytest.mark.parametrize(
    'contract',
    INSTALLER_CONTRACTS,
)
def test_supported_installer_commands_are_documented_and_smoke_tested(
    contract: InstallerContract,
    installer_smoke_action_text: str,
) -> None:
    """
    Test that supported installer commands stay aligned with release smoke
    coverage.
    """
    readme_text = README_PATH.read_text(encoding='utf-8')
    compatibility_text = COMPATIBILITY_PATH.read_text(encoding='utf-8')
    docs_command, smoke_pattern = contract

    assert docs_command in readme_text
    assert docs_command in compatibility_text
    assert smoke_pattern.search(installer_smoke_action_text) is not None
