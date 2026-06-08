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


type InstallerContract = tuple[str, re.Pattern[str]]
type InstallerStep = tuple[str, str]


# SECTION: CONSTANTS ======================================================== #


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
    'etlplus ui --help',
)
CONDA_STATUS_SNIPPETS = (
    'conda-forge',
    'tagged',
    'published',
)
PIP_VENV_PATH_SNIPPETS = (
    'if [[ "${RUNNER_OS}" == "Windows" ]]; then',
    'venv_python="${{ inputs.venv-path }}/Scripts/python.exe"',
    'etlplus_bin="${{ inputs.venv-path }}/Scripts/etlplus.exe"',
    'venv_python="${{ inputs.venv-path }}/bin/python"',
    'etlplus_bin="${{ inputs.venv-path }}/bin/etlplus"',
)
TOOL_INSTALLER_BIN_SNIPPETS = (
    'PIPX_BIN_DIR="${RUNNER_TEMP}/etlplus-pipx-bin"',
    'UV_TOOL_BIN_DIR="${RUNNER_TEMP}/etlplus-uv-bin"',
    'etlplus_bin="${PIPX_BIN_DIR}/etlplus"',
    'etlplus_bin="${PIPX_BIN_DIR}/etlplus.exe"',
    'etlplus_bin="${UV_TOOL_BIN_DIR}/etlplus"',
    'etlplus_bin="${UV_TOOL_BIN_DIR}/etlplus.exe"',
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
    return read_text(INSTALLER_SMOKE_ACTION_PATH)


@pytest.fixture(name='installer_smoke_steps', scope='module')
def installer_smoke_steps_fixture() -> tuple[InstallerStep, ...]:
    """Return installer smoke step names and scripts from the composite action."""
    action_data: dict[str, Any] = read_yaml(INSTALLER_SMOKE_ACTION_PATH)
    steps = action_data['runs']['steps']
    return tuple(
        (step['name'], step['run'])
        for step in steps
        if step.get('name', '').startswith('Smoke-test ')
    )


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


@pytest.mark.parametrize(
    ('step_name', 'expected_bin'),
    [
        pytest.param(
            'Smoke-test pip wheel install',
            '"$etlplus_bin"',
            id='pip',
        ),
        pytest.param(
            'Smoke-test pipx wheel install',
            '"$etlplus_bin"',
            id='pipx',
        ),
        pytest.param(
            'Smoke-test uv tool wheel install',
            '"$etlplus_bin"',
            id='uv',
        ),
    ],
)
def test_installer_smoke_checks_stable_cli_surfaces(
    installer_smoke_steps: tuple[InstallerStep, ...],
    step_name: str,
    expected_bin: str,
) -> None:
    """Test each installer verifies stable CLI version and help surfaces."""
    scripts_by_name = dict(installer_smoke_steps)
    script = scripts_by_name[step_name]

    assert f'{expected_bin} --version' in script
    assert f'{expected_bin} --help' in script
    assert f'{expected_bin} check --help' in script
    assert f'{expected_bin} ui --help' in script


def test_installer_smoke_keeps_expected_supported_installer_steps(
    installer_smoke_steps: tuple[InstallerStep, ...],
) -> None:
    """Test release smoke continues to cover all supported installer paths."""
    step_names = [name for name, _script in installer_smoke_steps]

    assert step_names == [
        'Smoke-test pip wheel install',
        'Smoke-test pipx wheel install',
        'Smoke-test uv tool wheel install',
    ]


@pytest.mark.parametrize('snippet', PIP_VENV_PATH_SNIPPETS)
def test_installer_smoke_resolves_pip_venv_paths_per_runner_os(
    installer_smoke_action_text: str,
    snippet: str,
) -> None:
    """Test pip wheel smoke uses OS-aware venv script paths."""
    assert snippet in installer_smoke_action_text


@pytest.mark.parametrize('snippet', TOOL_INSTALLER_BIN_SNIPPETS)
def test_installer_smoke_uses_explicit_tool_installer_entrypoint_paths(
    installer_smoke_action_text: str,
    snippet: str,
) -> None:
    """
    Test that tool-installer smoke checks do not rely on ambient PATH state.
    """
    assert '$HOME/.local/bin/etlplus' not in installer_smoke_action_text
    assert 'command -v etlplus' not in installer_smoke_action_text
    assert snippet in installer_smoke_action_text


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
    readme_text = read_text(README_PATH)
    compatibility_text = read_text(COMPATIBILITY_PATH)
    docs_command, smoke_pattern = contract

    assert docs_command in readme_text
    assert docs_command in compatibility_text
    assert smoke_pattern.search(installer_smoke_action_text) is not None
