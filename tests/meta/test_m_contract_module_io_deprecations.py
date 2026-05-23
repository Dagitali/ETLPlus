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

from etlplus.file import _registry as file_registry
from tests.pytest_shared_support import REPO_ROOT

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

_ETLPLUS_ROOT = REPO_ROOT / 'etlplus'


# SECTION: TYPE ALIASES ===================================================== #


type FileFormatModule = tuple[str, ModuleType]
type FileFormatModules = tuple[FileFormatModule, ...]
type RuntimeTree = tuple[Path, ast.AST]
type RuntimeTrees = tuple[RuntimeTree, ...]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _collect_imported_wrapper_aliases(
    path: Path,
    tree: ast.AST,
) -> tuple[dict[str, str], list[str]]:
    """
    Collect imported file-module aliases and report disallowed imports.
    """
    aliases: dict[str, str] = {}
    violations: list[str] = []
    for node in ast.walk(tree):
        match node:
            case ast.Import(names=names):
                for alias in names:
                    if alias.name in _MODULE_NAMES:
                        local_name = (
                            alias.asname
                            or alias.name.rsplit(
                                '.',
                                maxsplit=1,
                            )[-1]
                        )
                        aliases[local_name] = alias.name
            case ast.ImportFrom(module=module_name, names=names) if (
                module_name in _MODULE_NAMES
            ):
                for alias in names:
                    if alias.name in _WRAPPER_API_NAMES:
                        violations.append(
                            f'{path}:{node.lineno} imports {module_name}.{alias.name}',
                        )
            case ast.ImportFrom(module='etlplus.file', names=names):
                for alias in names:
                    if alias.name in _MODULE_SHORT_NAMES:
                        local_name = alias.asname or alias.name
                        aliases[local_name] = f'etlplus.file.{alias.name}'
            case _:
                continue
    return aliases, violations


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
        match node:
            case ast.Call(
                func=ast.Attribute(
                    attr=api_name,
                    value=ast.Name(id=alias_name),
                ),
            ) if api_name in _WRAPPER_API_NAMES:
                if module_name := imported_wrapper_aliases.get(alias_name):
                    violations.append(
                        f'{path}:{node.lineno} calls {module_name}.{api_name}()',
                    )
            case _:
                continue
    return violations


def _iter_internal_python_files() -> list[Path]:
    """Return non-file-subpackage runtime Python modules under ``etlplus``."""
    return [
        path
        for path in sorted(_ETLPLUS_ROOT.rglob('*.py'))
        if path.is_file()
        if path.relative_to(_ETLPLUS_ROOT).parts[:1] != ('file',)
    ]


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='file_format_modules', scope='module')
def file_format_modules_fixture() -> FileFormatModules:
    """Return mapped file-format modules imported once per test module."""
    return tuple(
        (module_name, importlib.import_module(module_name))
        for module_name in _MODULE_NAMES
    )


@pytest.fixture(name='internal_runtime_trees', scope='module')
def internal_runtime_trees_fixture() -> RuntimeTrees:
    """Return parsed AST trees for non-file runtime modules."""
    return tuple(
        (path, ast.parse(path.read_text(encoding='utf-8')))
        for path in _iter_internal_python_files()
    )


# SECTION: TESTS ============================================================ #


class TestFileFormatModules:
    """Unit tests for mapped file format module export contracts."""

    @pytest.mark.parametrize('api_name', _WRAPPER_API_NAMES)
    def test_no_io_exports(
        self,
        api_name: str,
        file_format_modules: FileFormatModules,
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
        file_format_modules: FileFormatModules,
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
        internal_runtime_trees: RuntimeTrees,
    ) -> None:
        """
        Test that runtime modules not importing/calling removed wrapper APIs.
        """
        violations: list[str] = []

        for path, tree in internal_runtime_trees:
            imported_wrapper_aliases, import_violations = (
                _collect_imported_wrapper_aliases(path, tree)
            )
            violations.extend(import_violations)
            violations.extend(
                _collect_wrapper_call_violations(
                    path,
                    tree,
                    imported_wrapper_aliases,
                ),
            )

        assert not violations, '\n'.join(violations)
