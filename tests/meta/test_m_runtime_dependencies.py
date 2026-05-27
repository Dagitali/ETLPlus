"""
:mod:`tests.meta.test_m_runtime_dependencies` module.

Meta tests for runtime dependency declarations.
"""

from __future__ import annotations

import ast
import re
import sys
import tomllib
from pathlib import Path

from tests.pytest_shared_support import REPO_ROOT

# SECTION: INTERNAL CONSTANTS =============================================== #


_REQUIREMENT_NAME_PATTERN = re.compile(r'^[A-Za-z0-9_.-]+')


# SECTION: CONSTANTS ======================================================== #


PYPROJECT_PATH = REPO_ROOT / 'pyproject.toml'
CLI_PACKAGE_PATH = REPO_ROOT / 'etlplus' / 'cli'
RUNTIME_IMPORT_DISTRIBUTIONS = {
    'click': 'click',
    'typer': 'typer',
}


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _canonical_requirement_name(requirement: str) -> str:
    """Return the canonical package name from one dependency requirement."""
    match = _REQUIREMENT_NAME_PATTERN.match(requirement)
    if match is None:
        raise ValueError(f'Invalid requirement: {requirement!r}')
    return match.group(0).casefold().replace('_', '-')


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


def _pyproject_dependency_names() -> set[str]:
    """Return base dependency names declared by ``pyproject.toml``."""
    pyproject = tomllib.loads(PYPROJECT_PATH.read_text(encoding='utf-8'))
    return {
        _canonical_requirement_name(requirement)
        for requirement in pyproject['project']['dependencies']
    }


# SECTION: TESTS ============================================================ #


class TestRuntimeDependencyDeclarations:
    """Meta tests for supported runtime import dependency declarations."""

    def test_cli_direct_runtime_imports_are_declared_base_dependencies(self) -> None:
        """Test direct CLI imports are declared as base runtime dependencies."""
        direct_imports = _direct_external_imports(CLI_PACKAGE_PATH)
        assert direct_imports <= RUNTIME_IMPORT_DISTRIBUTIONS.keys()

        expected = {
            RUNTIME_IMPORT_DISTRIBUTIONS[module]
            for module in direct_imports
        }

        assert expected <= _pyproject_dependency_names()
