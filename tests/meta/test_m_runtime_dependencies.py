"""
:mod:`tests.meta.test_m_runtime_dependencies` module.

Meta tests for runtime dependency declarations.
"""

from __future__ import annotations

import ast
import sys
import tomllib
from pathlib import Path

from tests.meta.pytest_meta_support import canonical_requirement_name
from tests.pytest_shared_support import REPO_ROOT

# SECTION: CONSTANTS ======================================================== #


CLI_PACKAGE_PATH = REPO_ROOT / 'etlplus' / 'cli'
PYPROJECT_PATH = REPO_ROOT / 'pyproject.toml'
BRANCH_PROTECTION_PATH = REPO_ROOT / '.github' / 'BRANCH-PROTECTION.md'
CI_CD_WORKFLOWS_PATH = REPO_ROOT / 'CI-CD-WORKFLOWS.md'
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
        tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name.partition('.')[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imports.add(node.module.partition('.')[0])

    return imports - sys.stdlib_module_names - {'etlplus'}


def _normalized_text(value: str) -> str:
    """Return case-folded text with Markdown line wrapping normalized."""
    return ' '.join(value.casefold().split())


def _pyproject_dependency_names() -> set[str]:
    """Return base dependency names declared by ``pyproject.toml``."""
    pyproject = tomllib.loads(PYPROJECT_PATH.read_text(encoding='utf-8'))
    return {
        canonical_requirement_name(requirement)
        for requirement in pyproject['project']['dependencies']
    }


# SECTION: TESTS ============================================================ #


class TestRuntimeDependencyDeclarations:
    """Meta tests for supported runtime import dependency declarations."""

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
        workflow_text = SBOM_WORKFLOW_PATH.read_text(encoding='utf-8')

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
        release_notes_text = _normalized_text(
            RELEASE_NOTES_TEMPLATE_PATH.read_text(encoding='utf-8'),
        )

        assert '`pyproject.toml`' in release_notes_text
        assert 'canonical package metadata source' in release_notes_text
        assert 'built distribution artifacts' in release_notes_text

    def test_release_workflow_docs_preserve_artifact_validation_path(self) -> None:
        """Test workflow docs keep release validation responsibilities explicit."""
        ci_cd_text = _normalized_text(
            CI_CD_WORKFLOWS_PATH.read_text(encoding='utf-8'),
        )

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
        pr_workflow_text = PR_WORKFLOW_PATH.read_text(encoding='utf-8')
        ci_cd_text = _normalized_text(
            CI_CD_WORKFLOWS_PATH.read_text(encoding='utf-8'),
        )
        branch_protection_text = BRANCH_PROTECTION_PATH.read_text(encoding='utf-8')

        assert 'name: Check uv lockfile' in pr_workflow_text
        assert 'run: uv lock --check' in pr_workflow_text
        assert 'committed `uv.lock` freshness against `pyproject.toml`' in ci_cd_text
        assert '- `check uv lockfile`' in ci_cd_text
        assert '- `Check uv lockfile`' in branch_protection_text
