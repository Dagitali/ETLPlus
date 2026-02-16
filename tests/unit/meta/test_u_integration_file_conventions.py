"""
:mod:`tests.unit.meta.test_u_integration_file_conventions` module.

Guardrails for integration file-smoke contract conventions.
"""

from __future__ import annotations

import ast
from pathlib import Path

from tests.integration.file.pytest_smoke_file_contracts import (
    SMOKE_ROUNDTRIP_EXCEPTION_MODULES,
)
from tests.integration.file.pytest_smoke_file_contracts import (
    SMOKE_ROUNDTRIP_EXCEPTION_OVERRIDES,
)
from tests.integration.file.pytest_smoke_file_contracts import (
    SMOKE_ROUNDTRIP_OVERRIDE_ATTRS,
)

# SECTION: INTERNAL CONSTANTS =============================================== #


_REPO_ROOT = Path(__file__).resolve().parents[3]
_INTEGRATION_FILE_ROOT = _REPO_ROOT / 'tests' / 'integration' / 'file'
_INTEGRATION_FILE_PATTERN = 'test_i_file_*.py'


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _class_base_name(node: ast.expr) -> str | None:
    """Return one class-base symbol name."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _is_contract_test_class(class_node: ast.ClassDef) -> bool:
    """Return whether one class inherits ``SmokeRoundtripModuleContract``."""
    return any(
        _class_base_name(base) == 'SmokeRoundtripModuleContract'
        for base in class_node.bases
    )


def _class_assigned_names(class_node: ast.ClassDef) -> set[str]:
    """Return assigned class attribute names for one class body."""
    assigned: set[str] = set()
    for node in class_node.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    assigned.add(target.id)
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name):
                assigned.add(node.target.id)
    return assigned


def _contract_classes(path: Path) -> list[ast.ClassDef]:
    """Return contract test classes from one integration test module."""
    tree = ast.parse(path.read_text(encoding='utf-8'))
    return [
        node
        for node in tree.body
        if (
            isinstance(node, ast.ClassDef)
            and node.name.startswith('Test')
            and _is_contract_test_class(node)
        )
    ]


def _module_override_attrs(path: Path) -> set[str]:
    """Return roundtrip override attrs used by one module contract class."""
    attrs: set[str] = set()
    for class_node in _contract_classes(path):
        attrs.update(
            _class_assigned_names(class_node) & SMOKE_ROUNDTRIP_OVERRIDE_ATTRS,
        )
    return attrs


def _integration_file_test_modules() -> list[Path]:
    """Return sorted integration file smoke test module paths."""
    return sorted(_INTEGRATION_FILE_ROOT.glob(_INTEGRATION_FILE_PATTERN))


# SECTION: TESTS ============================================================ #


class TestIntegrationFileSmokeConventions:
    """Guardrails for integration file-smoke contract symmetry."""

    def test_all_modules_use_smoke_roundtrip_contract(self) -> None:
        """Test each integration file module uses one contract test class."""
        missing_contract = sorted(
            path.name
            for path in _integration_file_test_modules()
            if not _contract_classes(path)
        )
        assert not missing_contract, (
            'Integration file tests without SmokeRoundtripModuleContract:\n- '
            + '\n- '.join(missing_contract)
        )

    def test_each_module_has_exactly_one_contract_test_class(self) -> None:
        """Test each integration file module defining one contract class."""
        offenders = sorted(
            f'{path.name}: {len(contract_classes)}'
            for path in _integration_file_test_modules()
            if len(contract_classes := _contract_classes(path)) != 1
        )
        assert not offenders, (
            'Integration file modules must define exactly one contract '
            'test class:\n- '
            + '\n- '.join(offenders)
        )

    def test_only_documented_modules_use_override_attributes(self) -> None:
        """Test only documented exception modules using override attrs."""
        overrides_by_module = {
            path.name: _module_override_attrs(path)
            for path in _integration_file_test_modules()
        }
        observed_exception_modules = {
            module_name
            for module_name, attrs in overrides_by_module.items()
            if attrs
        }
        assert observed_exception_modules == SMOKE_ROUNDTRIP_EXCEPTION_MODULES

        for module_name, attrs in overrides_by_module.items():
            if module_name in SMOKE_ROUNDTRIP_EXCEPTION_MODULES:
                assert (
                    attrs
                    == SMOKE_ROUNDTRIP_EXCEPTION_OVERRIDES[module_name]
                )
            else:
                assert not attrs
