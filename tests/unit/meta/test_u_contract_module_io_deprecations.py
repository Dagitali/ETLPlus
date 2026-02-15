"""
:mod:`tests.unit.meta.test_u_contract_module_io_deprecations` module.

Contract tests for deprecated module-level file I/O wrappers.
"""

from __future__ import annotations

import ast
import importlib
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from etlplus.file import registry as file_registry

# SECTION: MARKS ============================================================ #


pytestmark = pytest.mark.filterwarnings(
    (
        'default:.*is deprecated; use handler instance methods '
        'instead\\.:DeprecationWarning'
    ),
)


# SECTION: TYPE ALIASES ===================================================== #


type HandlerInstance = Any


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


def _get_module_handler(
    module: ModuleType,
) -> HandlerInstance:
    """Return the single module-level handler singleton instance."""
    handlers = [
        value
        for name, value in vars(module).items()
        if name.endswith('_HANDLER')
    ]
    assert len(handlers) == 1
    return handlers[0]


def _call_module_write(
    module: ModuleType,
    *,
    path: Path,
    payload: list[dict[str, object]],
) -> int:
    """Call one module-level write wrapper with module-specific kwargs."""
    if module.__name__ == 'etlplus.file.xml':
        return module.write(path, payload, root_tag='root')
    return module.write(path, payload)


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


@pytest.mark.parametrize(
    'module_name',
    _MODULE_NAMES,
)
def test_module_read_wrapper_warns_and_delegates(
    module_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test module read wrappers warning and delegating to handler instances.
    """
    module = importlib.import_module(module_name)
    handler = _get_module_handler(module)
    captured: dict[str, Path] = {}
    expected_result: object = {'ok': True}

    def fake_read(path: Path, *_args: object, **_kwargs: object) -> object:
        captured['path'] = path
        return expected_result

    monkeypatch.setattr(handler, 'read', fake_read)

    with pytest.warns(DeprecationWarning) as captured_warnings:
        result = module.read('sample.any')

    assert result is expected_result
    assert captured['path'] == Path('sample.any')
    assert any(
        f'{module_name}.read() is deprecated' in str(warning.message)
        for warning in captured_warnings.list
    )


@pytest.mark.parametrize(
    'module_name',
    _MODULE_NAMES,
)
def test_module_write_wrapper_warns_and_delegates(
    module_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test module write wrappers warning and delegating to handler instances.
    """
    module = importlib.import_module(module_name)
    handler = _get_module_handler(module)
    captured: dict[str, object] = {}
    payload: list[dict[str, object]] = [{'a': 1}]
    expected_count = 17

    def fake_write(
        path: Path,
        data: object,
        *_args: object,
        **_kwargs: object,
    ) -> int:
        captured['path'] = path
        captured['data'] = data
        return expected_count

    monkeypatch.setattr(handler, 'write', fake_write)

    with pytest.warns(DeprecationWarning) as captured_warnings:
        count = _call_module_write(
            module,
            path=Path('sample.any'),
            payload=payload,
        )

    assert count == expected_count
    assert captured['path'] == Path('sample.any')
    assert captured['data'] is payload
    assert any(
        f'{module_name}.write() is deprecated' in str(warning.message)
        for warning in captured_warnings.list
    )


def test_internal_runtime_code_does_not_use_module_level_io_wrappers() -> None:
    """Test runtime modules avoiding deprecated file-format module wrappers."""
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
