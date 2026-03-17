"""
:mod:`tests.meta.test_m_contract_module_io_deprecations` module.

Contract tests for removal of legacy module-level file I/O wrappers.
"""

from __future__ import annotations

import ast
import importlib
from pathlib import Path
from types import ModuleType

import pytest

from etlplus.file import registry as file_registry

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: INTERNAL CONSTANTS =============================================== #


_MODULE_NAMES: tuple[str, ...] = tuple(
    sorted(
        {
            spec.partition(':')[0]
            # pylint: disable-next=protected-access
            for spec in file_registry._HANDLER_CLASS_SPECS.values()
        },
    ),
)
_MODULE_SHORT_NAMES: frozenset[str] = frozenset(
    name.rsplit('.', maxsplit=1)[-1] for name in _MODULE_NAMES
)
_WRAPPER_API_NAMES: tuple[str, ...] = ('read', 'write')

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ETLPLUS_ROOT = _REPO_ROOT / 'etlplus'


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _collect_imported_wrapper_aliases(
    path: Path,
    tree: ast.AST,
    *,
    violations: list[str],
) -> dict[str, str]:
    """
    Collect imported file-module aliases and report disallowed imports.
    """
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in _MODULE_NAMES:
                    local_name = alias.asname or alias.name.rsplit('.', maxsplit=1)[-1]
                    aliases[local_name] = alias.name
            continue

        if not isinstance(node, ast.ImportFrom):
            continue
        if node.module in _MODULE_NAMES:
            for alias in node.names:
                if alias.name in _WRAPPER_API_NAMES:
                    violations.append(
                        f'{path}:{node.lineno} imports {node.module}.{alias.name}',
                    )
        if node.module == 'etlplus.file':
            for alias in node.names:
                if alias.name in _MODULE_SHORT_NAMES:
                    local_name = alias.asname or alias.name
                    aliases[local_name] = f'etlplus.file.{alias.name}'
    return aliases


def _collect_wrapper_call_violations(
    path: Path,
    tree: ast.AST,
    imported_wrapper_aliases: dict[str, str],
) -> list[str]:
    """
    Collect violations for calls to removed module-level wrapper APIs.
    """
    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in _WRAPPER_API_NAMES:
            continue
        if not isinstance(node.func.value, ast.Name):
            continue
        module_name = imported_wrapper_aliases.get(node.func.value.id)
        if module_name is None:
            continue
        violations.append(
            f'{path}:{node.lineno} calls {module_name}.{node.func.attr}()',
        )
    return violations


def _iter_internal_python_files() -> list[Path]:
    """Return non-file-subpackage runtime Python modules under ``etlplus``."""
    files: list[Path] = []
    for path in sorted(_ETLPLUS_ROOT.rglob('*.py')):
        if not path.is_file():
            continue
        relative_path = path.relative_to(_ETLPLUS_ROOT)
        if relative_path.parts and relative_path.parts[0] == 'file':
            continue
        files.append(path)
    return files


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='file_format_modules', scope='module')
def file_format_modules_fixture() -> list[tuple[str, ModuleType]]:
    """Return mapped file-format modules imported once per test module."""
    return [
        (module_name, importlib.import_module(module_name))
        for module_name in _MODULE_NAMES
    ]


@pytest.fixture(name='internal_runtime_trees', scope='module')
def internal_runtime_trees_fixture() -> list[tuple[Path, ast.AST]]:
    """Return parsed AST trees for non-file runtime modules."""
    return [
        (path, ast.parse(path.read_text(encoding='utf-8')))
        for path in _iter_internal_python_files()
    ]


# SECTION: TESTS ============================================================ #


class TestFileFormatModules:
    """Unit tests for mapped file format module export contracts."""

    @pytest.mark.parametrize('api_name', _WRAPPER_API_NAMES)
    def test_no_io_exports(
        self,
        api_name: str,
        file_format_modules: list[tuple[str, ModuleType]],
    ) -> None:
        """Test that mapped modules do not expose wrapper API attributes."""
        violations = [
            module_name
            for module_name, module in file_format_modules
            if hasattr(module, api_name)
        ]
        assert not violations, '\n'.join(violations)

    def test_no_handler_singletons(
        self,
        file_format_modules: list[tuple[str, ModuleType]],
    ) -> None:
        """Test that mapped modules do not expose ``_*_HANDLER`` constants."""
        violations = [
            f'{module_name}.{symbol_name}'
            for module_name, module in file_format_modules
            for symbol_name in vars(module)
            if symbol_name.endswith('_HANDLER')
        ]
        assert not violations, '\n'.join(violations)


class TestInternalRuntimeCode:
    """Unit tests for internal runtime usage of deprecated file APIs."""

    def test_no_removed_io_wrapper_usage(
        self,
        internal_runtime_trees: list[tuple[Path, ast.AST]],
    ) -> None:
        """
        Test that runtime modules not importing/calling removed wrapper APIs.
        """
        violations: list[str] = []

        for path, tree in internal_runtime_trees:
            imported_wrapper_aliases = _collect_imported_wrapper_aliases(
                path,
                tree,
                violations=violations,
            )
            violations.extend(
                _collect_wrapper_call_violations(
                    path,
                    tree,
                    imported_wrapper_aliases,
                ),
            )

        assert not violations, '\n'.join(violations)
