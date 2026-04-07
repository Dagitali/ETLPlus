"""
:mod:`tests.unit.pytest_export_contracts` module.

Pytest plugin for testing package facade and helper-module export contracts.
"""

from __future__ import annotations

import ast
from pathlib import Path
from types import ModuleType

# SECTION: FUNCTIONS ======================================================== #


def referenced_alias_attributes(
    *,
    module_path: Path,
    alias: str,
) -> list[str]:
    """Return sorted attribute names referenced via one module alias."""
    tree = ast.parse(module_path.read_text(encoding='utf-8'))
    return sorted(
        {
            node.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == alias
        },
    )


def assert_helper_module_exports_match_facade_usage(
    *,
    facade_module: ModuleType,
    helper_module: ModuleType,
    alias: str,
) -> None:
    """
    Assert that a helper module exports only names referenced by one facade.

    Parameters
    ----------
    facade_module : ModuleType
        The facade module that consumes helper names through one alias.
    helper_module : ModuleType
        The helper module whose ``__all__`` contract is under test.
    alias : str
        The import alias used for helper-module attribute access in the facade.
    """
    assert facade_module.__file__ is not None
    referenced_names = referenced_alias_attributes(
        module_path=Path(facade_module.__file__),
        alias=alias,
    )
    assert helper_module.__all__ == referenced_names
