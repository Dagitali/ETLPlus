"""
:mod:`tests.meta.test_m_runtime_dependencies` module.

Meta tests for runtime dependency declarations.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

from tests.meta.pytest_meta_support import canonical_requirement_name
from tests.meta.pytest_meta_support import normalized_text
from tests.meta.pytest_meta_support import read_text
from tests.meta.pytest_meta_support import read_toml
from tests.pytest_shared_support import REPO_ROOT

# SECTION: CONSTANTS ======================================================== #


CLI_PACKAGE_PATH = REPO_ROOT / 'etlplus' / 'cli'
PYPROJECT_PATH = REPO_ROOT / 'pyproject.toml'
BRANCH_PROTECTION_PATH = REPO_ROOT / '.github' / 'BRANCH-PROTECTION.md'
CI_CD_WORKFLOWS_PATH = REPO_ROOT / 'CI-CD-WORKFLOWS.md'
DEPENDENCY_POLICY_NOTES_PATH = REPO_ROOT / 'DEPENDENCY-AND-EXTENSION-POLICY-NOTES.md'
PR_WORKFLOW_PATH = REPO_ROOT / '.github' / 'workflows' / 'pr.yml'
RELEASE_NOTES_TEMPLATE_PATH = REPO_ROOT / '.github' / 'RELEASE-NOTES-TEMPLATE.md'
SBOM_WORKFLOW_PATH = REPO_ROOT / '.github' / 'workflows' / 'sbom.yml'

RUNTIME_IMPORT_DISTRIBUTIONS = {
    'click': 'click',
    'typer': 'typer',
}


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _direct_external_imports(package_path: Path) -> set[str]:
    """Return direct external top-level imports used by Python files."""
    imports: set[str] = set()
    for path in package_path.rglob('*.py'):
        tree = ast.parse(read_text(path), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name.partition('.')[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imports.add(node.module.partition('.')[0])

    return imports - sys.stdlib_module_names - {'etlplus'}


def _pyproject_dependency_names() -> set[str]:
    """Return base dependency names declared by ``pyproject.toml``."""
    pyproject = read_toml(PYPROJECT_PATH)
    return {
        canonical_requirement_name(requirement)
        for requirement in pyproject['project']['dependencies']
    }


def _snapshot_dependency_names() -> set[str]:
    """Return base dependency names from the design-note snapshot block."""
    text = read_text(DEPENDENCY_POLICY_NOTES_PATH)
    marker = '## Base Dependency Snapshot'
    section = text.split(marker, maxsplit=1)[1].split('## ', maxsplit=1)[0]
    block = section.split('```text', maxsplit=1)[1].split('```', maxsplit=1)[0]
    return {line.strip() for line in block.splitlines() if line.strip()}


# SECTION: TESTS ============================================================ #


class TestRuntimeDependencyDeclarations:
    """Meta tests for supported runtime import dependency declarations."""

    def test_base_dependencies_match_design_note_snapshot(self) -> None:
        """Test broad base dependency changes require an explicit design-note update."""
        assert _pyproject_dependency_names() == _snapshot_dependency_names()

    def test_cli_direct_runtime_imports_are_declared_base_dependencies(self) -> None:
        """Test direct CLI imports are declared as base runtime dependencies."""
        direct_imports = _direct_external_imports(CLI_PACKAGE_PATH)
        assert direct_imports <= RUNTIME_IMPORT_DISTRIBUTIONS.keys()

        expected = {RUNTIME_IMPORT_DISTRIBUTIONS[module] for module in direct_imports}

        assert expected <= _pyproject_dependency_names()


class TestToolDependencyDeclarations:
    """Meta tests for CI tool dependency declarations."""

    def test_sbom_workflow_installs_pinned_isolated_tool(self) -> None:
        """Test SBOM generation isolates the pinned tool from runtime deps."""
        workflow_text = read_text(SBOM_WORKFLOW_PATH)

        assert 'python-bootstrap: "."' in workflow_text
        assert "CYCLONEDX_BOM_VERSION: '7.2.2'" in workflow_text
        assert '-m pip install "cyclonedx-bom==${CYCLONEDX_BOM_VERSION}"' in (
            workflow_text
        )
        assert 'cyclonedx-bom-venv/bin/cyclonedx-py' in workflow_text
        assert 'environment "$(python -c \'import sys; print(sys.executable)\')"' in (
            workflow_text
        )
        assert 'python-bootstrap: ".[sbom]"' not in workflow_text
        assert 'python -m pip install cyclonedx-bom' not in workflow_text

    def test_release_notes_template_calls_out_lockfile_release_boundary(
        self,
    ) -> None:
        """Test release notes keep lockfile changes framed as maintenance."""
        release_notes_text = normalized_text(read_text(RELEASE_NOTES_TEMPLATE_PATH))

        assert '`pyproject.toml`' in release_notes_text
        assert 'canonical package metadata source' in release_notes_text
        assert 'built distribution artifacts' in release_notes_text

    def test_release_workflow_docs_preserve_artifact_validation_path(self) -> None:
        """Test workflow docs keep release validation responsibilities explicit."""
        ci_cd_text = normalized_text(read_text(CI_CD_WORKFLOWS_PATH))

        expected_snippets = (
            'build source and wheel distributions with `python -m build`',
            'audit release artifacts and validate them with `twine check`',
            'smoke-test supported installer paths against the built wheel',
            'smoke-test packaged behavior against the built wheel',
            'publish to pypi through trusted publishing',
        )

        assert all(snippet in ci_cd_text for snippet in expected_snippets)

    def test_uv_lockfile_gate_is_documented_for_required_checks(self) -> None:
        """Test PR lockfile gate stays reflected in workflow and branch docs."""
        pr_workflow_text = read_text(PR_WORKFLOW_PATH)
        ci_cd_text = normalized_text(read_text(CI_CD_WORKFLOWS_PATH))
        branch_protection_text = read_text(BRANCH_PROTECTION_PATH)

        assert 'name: Check uv lockfile' in pr_workflow_text
        assert 'run: uv lock --check' in pr_workflow_text
        assert 'committed `uv.lock` freshness against `pyproject.toml`' in ci_cd_text
        assert '- `check uv lockfile`' in ci_cd_text
        assert '- `Check uv lockfile`' in branch_protection_text
