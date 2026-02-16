"""
:mod:`tests.unit.meta.test_u_test_layout` module.

Guardrails for repository test-layout conventions.
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_LEGACY_SMOKE_ROOT = _REPO_ROOT / 'tests' / 'smoke'


def test_legacy_smoke_python_modules_not_present() -> None:
    """
    Test that legacy smoke-path Python modules are fully migrated.
    """
    assert not list(_LEGACY_SMOKE_ROOT.rglob('*.py'))
