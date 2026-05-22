"""
:mod:`tests.meta.test_m_test_layout` module.

Guardrails for repository test-layout conventions.
"""

from __future__ import annotations

from tests.meta.pytest_meta_support import REPO_ROOT

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

_LEGACY_SMOKE_ROOT = REPO_ROOT / 'tests' / 'smoke'


def test_legacy_smoke_python_modules_not_present() -> None:
    """
    Test that legacy smoke-path Python modules are fully migrated.
    """
    assert not list(_LEGACY_SMOKE_ROOT.rglob('*.py'))
