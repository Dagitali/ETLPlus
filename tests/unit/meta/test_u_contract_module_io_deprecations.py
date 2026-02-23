"""
:mod:`tests.unit.meta.test_u_contract_module_io_deprecations` module.

Contract tests for removal of legacy module-level file I/O wrappers.
"""

from __future__ import annotations

import ast
import importlib
from pathlib import Path

from etlplus.file import registry as file_registry

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

_REPO_ROOT = Path(__file__).resolve().parents[3]
_ETLPLUS_ROOT = _REPO_ROOT / 'etlplus'


# SECTION: INTERNAL FUNCTIONS =============================================== #


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


# SECTION: TESTS ============================================================ #


def test_file_format_modules_do_not_expose_module_level_read_write() -> None:
    """
    Test mapped file modules no longer exposing ``read``/``write`` wrappers.
    """
    for module_name in _MODULE_NAMES:
        module = importlib.import_module(module_name)
        assert not hasattr(module, 'read'), module_name
        assert not hasattr(module, 'write'), module_name


def test_internal_runtime_code_does_not_use_removed_module_io_wrappers(
) -> None:
    """Test runtime modules not importing/calling removed wrapper APIs."""
    violations: list[str] = []

    for path in _iter_internal_python_files():
        tree = ast.parse(path.read_text(encoding='utf-8'))
        imported_wrapper_aliases: dict[str, str] = {}

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name in _MODULE_NAMES:
                        local_name = (
                            alias.asname
                            or alias.name.rsplit('.', maxsplit=1)[-1]
                        )
                        imported_wrapper_aliases[local_name] = alias.name

            if isinstance(node, ast.ImportFrom):
                if node.module in _MODULE_NAMES:
                    for alias in node.names:
                        if alias.name in {'read', 'write'}:
                            violations.append(
                                f'{path}:{node.lineno} imports '
                                f'{node.module}.{alias.name}',
                            )
                if node.module == 'etlplus.file':
                    for alias in node.names:
                        if alias.name in _MODULE_SHORT_NAMES:
                            local_name = alias.asname or alias.name
                            imported_wrapper_aliases[local_name] = (
                                f'etlplus.file.{alias.name}'
                            )

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in {'read', 'write'}:
                continue
            if not isinstance(node.func.value, ast.Name):
                continue
            module_name = imported_wrapper_aliases.get(node.func.value.id)
            if module_name is None:
                continue
            violations.append(
                f'{path}:{node.lineno} calls {module_name}.{node.func.attr}()',
            )

    assert not violations, '\n'.join(violations)
