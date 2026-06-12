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
type InstallerSmokeStep = tuple[str, str]
type InstallerStep = tuple[str, str]


# SECTION: CONSTANTS ======================================================== #


RELEASE_CHECKLIST_PATH = REPO_ROOT / 'RELEASE-CHECKLIST.md'
COMPATIBILITY_PATH = REPO_ROOT / 'docs/source/getting-started/compatibility.md'

BUILD_AND_UPLOAD_DIST_ACTION_PATH = (
    REPO_ROOT / '.github/actions/build-and-upload-dist/action.yml'
)
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
    'VENV_PATH: ${{ inputs.venv-path }}',
    'if [[ "${RUNNER_OS}" == "Windows" ]]; then',
    'venv_bin="${VENV_PATH}/Scripts"',
    'venv_python="${venv_bin}/python.exe"',
    'venv_bin="${VENV_PATH}/bin"',
    'venv_python="${venv_bin}/python"',
    'run_smoke_commands "$venv_bin"',
)
TOOL_INSTALLER_BIN_SNIPPETS = (
    'PIPX_BIN_DIR="${RUNNER_TEMP}/installer-smoke-pipx-bin"',
    'UV_TOOL_BIN_DIR="${RUNNER_TEMP}/installer-smoke-uv-bin"',
    'run_smoke_commands "$PIPX_BIN_DIR"',
    'run_smoke_commands "$UV_TOOL_BIN_DIR"',
)
STABLE_CLI_SURFACE_ARGS = (
    '--version',
    '--help',
    'check --help',
    'ui --help',
)
SUPPORTED_INSTALLER_STEPS: tuple[InstallerSmokeStep, ...] = (
    ('Smoke-test pip wheel install', '"$etlplus_bin"'),
    ('Smoke-test pipx wheel install', '"$etlplus_bin"'),
    ('Smoke-test uv tool wheel install', '"$etlplus_bin"'),
)


INSTALLER_CONTRACTS = (
    pytest.param(
        (
            'pip install etlplus',
            re.compile(r'-m pip install \$ARTIFACT_WHEEL'),
        ),
        id='pip',
    ),
    pytest.param(
        (
            'pipx install etlplus',
            re.compile(r'-m pipx install \$ARTIFACT_WHEEL'),
        ),
        id='pipx',
    ),
    pytest.param(
        (
            'uv tool install etlplus',
            re.compile(r'uv tool install --force \$ARTIFACT_WHEEL'),
        ),
        id='uv',
    ),
)

CONDA_STATUS_CASES = tuple(
    pytest.param(path, snippet, id=f'{path.name}:{snippet}')
    for path in CONDA_STATUS_DOC_PATHS
    for snippet in CONDA_STATUS_SNIPPETS
)
INSTALLER_SMOKE_SURFACE_CASES = tuple(
    pytest.param(
        step_name,
        expected_bin,
        cli_args,
        id=(
            f'{step_name.removeprefix("Smoke-test ").removesuffix(" wheel install")}:'
            f'{cli_args}'
        ),
    )
    for step_name, expected_bin in SUPPORTED_INSTALLER_STEPS
    for cli_args in STABLE_CLI_SURFACE_ARGS
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


@pytest.mark.parametrize('cli_args', STABLE_CLI_SURFACE_ARGS)
def test_installer_smoke_checks_stable_cli_surfaces(
    installer_smoke_action_text: str,
    cli_args: str,
) -> None:
    """Test ETLPlus release smoke preserves stable CLI version and help surfaces."""
    build_action: dict[str, Any] = read_yaml(BUILD_AND_UPLOAD_DIST_ACTION_PATH)
    smoke_commands = build_action['inputs']['smoke-commands']['default']
    build_installer_step = next(
        step
        for step in build_action['runs']['steps']
        if step.get('uses') == './.github/actions/installer-smoke'
    )

    assert f'etlplus {cli_args}' in smoke_commands
    assert (
        build_installer_step['with']['smoke-commands'] == '${{ inputs.smoke-commands }}'
    )
    assert 'run_smoke_commands' in installer_smoke_action_text
    assert 'eval "$smoke_command"' in installer_smoke_action_text


def test_installer_smoke_keeps_expected_supported_installer_steps(
    installer_smoke_action_text: str,
) -> None:
    """Test release smoke continues to cover all supported installer paths."""
    build_action: dict[str, Any] = read_yaml(BUILD_AND_UPLOAD_DIST_ACTION_PATH)
    installer_action: dict[str, Any] = read_yaml(INSTALLER_SMOKE_ACTION_PATH)

    build_inputs = build_action['inputs']
    installer_inputs = installer_action['inputs']
    build_installer_step = next(
        step
        for step in build_action['runs']['steps']
        if step.get('uses') == './.github/actions/installer-smoke'
    )

    assert installer_inputs['installer-smoke-installers']['default'] == 'pip,pipx,uv'
    assert build_inputs['installer-smoke-installers']['default'] == 'pip,pipx,uv'
    assert (
        build_installer_step['with']['installer-smoke-installers']
        == '${{ inputs.installer-smoke-installers }}'
    )

    for installer in ('pip', 'pipx', 'uv'):
        assert f'if [[ "$installer_list" == *",{installer},"* ]]' in (
            installer_smoke_action_text
        )
    assert 'pip|pipx|uv)' in installer_smoke_action_text


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
    assert '$HOME/.local/bin' not in installer_smoke_action_text
    assert 'command -v' not in installer_smoke_action_text
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
    assert 'ARTIFACT_WHEEL: ${{ inputs.artifact-wheel }}' in installer_smoke_action_text
    assert smoke_pattern.search(installer_smoke_action_text) is not None
