"""
:mod:`tests.meta.test_m_test_layout` module.

Guardrails for repository test-layout conventions.
"""

from __future__ import annotations

from tests.pytest_shared_support import TESTS_ROOT

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


def test_legacy_smoke_python_modules_not_present() -> None:
    """
    Test that legacy smoke-path Python modules are fully migrated.
    """
    assert not list((TESTS_ROOT / 'smoke').rglob('*.py'))
