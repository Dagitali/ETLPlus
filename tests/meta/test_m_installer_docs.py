"""
:mod:`tests.meta.test_m_installer_docs` module.

Guardrails for supported installer documentation and release smoke coverage.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pytest

from tests.meta.pytest_meta_support import REPO_ROOT

# SECTION: CONSTANTS ======================================================== #


README_PATH = REPO_ROOT / 'README.md'
RELEASE_CHECKLIST_PATH = REPO_ROOT / 'RELEASE-CHECKLIST.md'
COMPATIBILITY_PATH = REPO_ROOT / 'docs/source/getting-started/compatibility.md'

CI_WORKFLOW_PATH = REPO_ROOT / '.github/workflows/ci.yml'
INSTALLER_SMOKE_ACTION_PATH = REPO_ROOT / '.github/actions/installer-smoke/action.yml'


# SECTION: SUPPORT ========================================================== #


@dataclass(frozen=True, slots=True)
class InstallerContract:
    """Documented installer command and matching smoke-action pattern."""

    name: str
    docs_command: str
    smoke_pattern: re.Pattern[str]


INSTALLER_CONTRACTS = (
    InstallerContract(
        name='pip',
        docs_command='pip install etlplus',
        smoke_pattern=re.compile(r'-m pip install \$\{\{ inputs\.artifact-wheel \}\}'),
    ),
    InstallerContract(
        name='pipx',
        docs_command='pipx install etlplus',
        smoke_pattern=re.compile(r'-m pipx install \$\{\{ inputs\.artifact-wheel \}\}'),
    ),
    InstallerContract(
        name='uv',
        docs_command='uv tool install etlplus',
        smoke_pattern=re.compile(
            r'uv tool install --force \$\{\{ inputs\.artifact-wheel \}\}',
        ),
    ),
)


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='compatibility_text', scope='module')
def compatibility_text_fixture() -> str:
    """Return the published compatibility page source."""
    return COMPATIBILITY_PATH.read_text(encoding='utf-8')


@pytest.fixture(name='installer_smoke_action_text', scope='module')
def installer_smoke_action_text_fixture() -> str:
    """Return the supported installer smoke action source."""
    return INSTALLER_SMOKE_ACTION_PATH.read_text(encoding='utf-8')


@pytest.fixture(name='readme_text', scope='module')
def readme_text_fixture() -> str:
    """Return the repository README text."""
    return README_PATH.read_text(encoding='utf-8')


# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    'path',
    (README_PATH, COMPATIBILITY_PATH, RELEASE_CHECKLIST_PATH),
    ids=lambda path: path.name,
)
def test_conda_status_is_documented_as_validated_but_unpublished(path: Path) -> None:
    """
    Test that conda packaging is documented as support-gate validated without
    claiming user-facing install availability before feedstock publication.
    """
    text = path.read_text(encoding='utf-8')
    normalized_text = text.lower()

    assert 'conda-forge' in normalized_text
    assert 'tagged' in normalized_text
    assert 'published' in normalized_text


def test_cross_platform_smoke_checks_cli_help_surfaces() -> None:
    """Test macOS/Windows smoke coverage checks stable CLI help surfaces."""
    workflow_text = CI_WORKFLOW_PATH.read_text(encoding='utf-8')

    assert 'os: [macos-latest, windows-latest]' in workflow_text
    assert 'etlplus --version' in workflow_text
    assert 'etlplus --help' in workflow_text
    assert 'etlplus check --help' in workflow_text


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
    ids=lambda contract: contract.name,
)
def test_supported_installer_commands_are_documented_and_smoke_tested(
    contract: InstallerContract,
    readme_text: str,
    compatibility_text: str,
    installer_smoke_action_text: str,
) -> None:
    """
    Test that supported installer commands stay aligned with release smoke
    coverage.
    """
    assert contract.docs_command in readme_text
    assert contract.docs_command in compatibility_text
    assert contract.smoke_pattern.search(installer_smoke_action_text) is not None
