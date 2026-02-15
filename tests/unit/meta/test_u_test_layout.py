"""
:mod:`tests.unit.meta.test_u_test_layout` module.

Guardrails for repository test-layout conventions.
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SMOKE_ROOT = _REPO_ROOT / 'tests' / 'smoke'


def test_legacy_smoke_cli_folder_not_present() -> None:
    """
    Test that migrated CLI smoke tests stay out of legacy smoke paths.
    """
    assert not (_SMOKE_ROOT / 'cli').exists()
