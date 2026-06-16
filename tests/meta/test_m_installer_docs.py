"""
:mod:`tests.meta.test_m_installer_docs` module.

Guardrails for supported installer documentation and release smoke coverage.
"""

from __future__ import annotations

import re
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

PYTHON_PROJECT_LIFECYCLE_ACTION_PATH = (
    REPO_ROOT / '.github/actions/python-project-lifecycle/action.yml'
)
CI_WORKFLOW_PATH = REPO_ROOT / '.github/workflows/ci.yml'
CD_WORKFLOW_PATH = REPO_ROOT / '.github/workflows/cd.yml'

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


def _python_project_lifecycle_action() -> dict[str, Any]:
    """Return parsed Python project lifecycle action metadata."""
    return read_yaml(PYTHON_PROJECT_LIFECYCLE_ACTION_PATH)


def _lifecycle_installer_smoke_step() -> dict[str, Any]:
    """Return the lifecycle action step invoking external installer smoke."""
    lifecycle_action = _python_project_lifecycle_action()
    return next(
        step
        for step in lifecycle_action['runs']['steps']
        if str(step.get('uses', '')).startswith('Dagitali/python-installer-smoke@')
    )


def _workflow_lifecycle_build_upload_steps() -> tuple[dict[str, Any], ...]:
    """Return workflow steps invoking the lifecycle build-upload phase."""
    steps: list[dict[str, Any]] = []
    for workflow_path in (CI_WORKFLOW_PATH, CD_WORKFLOW_PATH):
        workflow = read_yaml(workflow_path)
        for job in workflow['jobs'].values():
            for step in job['steps']:
                if step.get('uses') != './.github/actions/python-project-lifecycle':
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
    lifecycle_installer_step = _lifecycle_installer_smoke_step()
    workflow_build_steps = _workflow_lifecycle_build_upload_steps()

    assert workflow_build_steps
    assert all(
        f'etlplus {cli_args}' in step['with']['smoke-commands']
        for step in workflow_build_steps
    )
    assert (
        lifecycle_installer_step['with']['smoke-commands']
        == '${{ inputs.smoke-commands }}'
    )


def test_installer_smoke_keeps_expected_supported_installer_steps() -> None:
    """Test release smoke continues to cover all supported installer paths."""
    lifecycle_action = _python_project_lifecycle_action()
    lifecycle_inputs = lifecycle_action['inputs']
    lifecycle_installer_step = _lifecycle_installer_smoke_step()

    assert re.fullmatch(
        r'Dagitali/python-installer-smoke@[0-9a-f]{40}',
        lifecycle_installer_step['uses'],
    )
    assert lifecycle_inputs['installer-smoke-installers']['default'] == 'pip,pipx,uv'
    assert (
        lifecycle_installer_step['with']['installer-smoke-installers']
        == '${{ inputs.installer-smoke-installers }}'
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
    lifecycle_action = _python_project_lifecycle_action()
    docs_command, installer_name = contract

    assert docs_command in readme_text
    assert docs_command in compatibility_text
    assert installer_name in (
        lifecycle_action['inputs']['installer-smoke-installers']['default'].split(',')
    )
