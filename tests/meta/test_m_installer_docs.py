"""
:mod:`tests.meta.test_m_installer_docs` module.

Guardrails for supported installer documentation and release smoke coverage.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests.meta.pytest_meta_support import README_PATH
from tests.meta.pytest_meta_support import read_text
from tests.meta.pytest_meta_support import read_yaml
from tests.pytest_shared_support import REPO_ROOT

# SECTION: TYPES ============================================================ #


type InstallerDocContract = tuple[str, str]


# SECTION: CONSTANTS ======================================================== #


RELEASE_CHECKLIST_PATH = REPO_ROOT / 'RELEASE-CHECKLIST.md'
COMPATIBILITY_PATH = REPO_ROOT / 'docs/source/getting-started/compatibility.md'

CI_WORKFLOW_PATH = REPO_ROOT / '.github/workflows/ci.yml'
CD_WORKFLOW_PATH = REPO_ROOT / '.github/workflows/cd.yml'

PYTHON_PROJECT_LIFECYCLE_ACTION_REF = (
    'Dagitali/python-project-lifecycle@3830839be6caca98cacca702c5aa38805c4fb516'
)

EXPECTED_INSTALLER_SMOKE_INSTALLERS = 'pip,pipx,uv'

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
    'etlplus ui --help',
)
CONDA_STATUS_SNIPPETS = (
    'conda-forge',
    'tagged',
    'published',
)
STABLE_CLI_SURFACE_ARGS = (
    '--version',
    '--help',
    'check --help',
    'ui --help',
)

INSTALLER_CONTRACTS = (
    pytest.param(
        (
            'pip install etlplus',
            'pip',
        ),
        id='pip',
    ),
    pytest.param(
        (
            'pipx install etlplus',
            'pipx',
        ),
        id='pipx',
    ),
    pytest.param(
        (
            'uv tool install etlplus',
            'uv',
        ),
        id='uv',
    ),
)

CONDA_STATUS_CASES = tuple(
    pytest.param(path, snippet, id=f'{path.name}:{snippet}')
    for path in CONDA_STATUS_DOC_PATHS
    for snippet in CONDA_STATUS_SNIPPETS
)
# SECTION: INTERNAL FUNCTIONS =============================================== #


def _workflow_lifecycle_build_upload_steps() -> tuple[dict[str, Any], ...]:
    """Return workflow steps invoking the lifecycle build-upload phase."""
    steps: list[dict[str, Any]] = []
    for workflow_path in (CI_WORKFLOW_PATH, CD_WORKFLOW_PATH):
        workflow = read_yaml(workflow_path)
        for job in workflow['jobs'].values():
            for step in job['steps']:
                if step.get('uses') != PYTHON_PROJECT_LIFECYCLE_ACTION_REF:
                    continue
                if step.get('with', {}).get('phase') == 'build-upload':
                    steps.append(step)
    return tuple(steps)


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
    normalized_text = read_text(path).lower()

    assert snippet in normalized_text


@pytest.mark.parametrize('snippet', CROSS_PLATFORM_SMOKE_SNIPPETS)
def test_cross_platform_smoke_checks_cli_help_surfaces(snippet: str) -> None:
    """Test macOS/Windows smoke coverage checks stable CLI help surfaces."""
    workflow_text = read_text(CI_WORKFLOW_PATH)

    assert snippet in workflow_text


@pytest.mark.parametrize('cli_args', STABLE_CLI_SURFACE_ARGS)
def test_installer_smoke_checks_stable_cli_surfaces(
    cli_args: str,
) -> None:
    """Test ETLPlus release smoke preserves stable CLI version and help surfaces."""
    workflow_build_steps = _workflow_lifecycle_build_upload_steps()

    assert workflow_build_steps
    assert all(
        f'etlplus {cli_args}' in step['with']['smoke-commands']
        for step in workflow_build_steps
    )


def test_installer_smoke_keeps_expected_supported_installer_steps() -> None:
    """Test release smoke continues to cover all supported installer paths."""
    workflow_build_steps = _workflow_lifecycle_build_upload_steps()

    assert workflow_build_steps
    assert all(
        step['uses'] == PYTHON_PROJECT_LIFECYCLE_ACTION_REF
        for step in workflow_build_steps
    )
    assert all(
        step['with']['installer-smoke-installers']
        == EXPECTED_INSTALLER_SMOKE_INSTALLERS
        for step in workflow_build_steps
    )


@pytest.mark.parametrize(
    'contract',
    INSTALLER_CONTRACTS,
)
def test_supported_installer_commands_are_documented_and_smoke_tested(
    contract: InstallerDocContract,
) -> None:
    """
    Test that supported installer commands stay aligned with release smoke
    coverage.
    """
    readme_text = read_text(README_PATH)
    compatibility_text = read_text(COMPATIBILITY_PATH)
    workflow_build_steps = _workflow_lifecycle_build_upload_steps()
    docs_command, installer_name = contract

    assert docs_command in readme_text
    assert docs_command in compatibility_text
    assert workflow_build_steps
    assert all(
        installer_name in step['with']['installer-smoke-installers'].split(',')
        for step in workflow_build_steps
    )
