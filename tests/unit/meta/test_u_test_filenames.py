"""
:mod:`tests.unit.meta.test_u_test_filenames` module.

Guardrails for test-module filename conventions.
"""

from __future__ import annotations

import re
from pathlib import Path

# SECTION: INTERNAL CONSTANTS =============================================== #


_REPO_ROOT = Path(__file__).resolve().parents[3]
_TESTS_ROOT = _REPO_ROOT / 'tests'
_DUPLICATE_SUFFIX_PATTERN = re.compile(r'\s+\d+\.py$')


# SECTION: TESTS ============================================================ #


def test_python_test_filenames_have_no_spaces_or_numbered_duplicates() -> None:
    """
    Test that test-related Python filenames avoid spaces and ``" 2.py"``-style
    duplicate suffixes.
    """
    offenders = sorted(
        path.relative_to(_REPO_ROOT).as_posix()
        for path in _TESTS_ROOT.rglob('*.py')
        if (
            '__pycache__' not in path.parts
            and (
                ' ' in path.name
                or _DUPLICATE_SUFFIX_PATTERN.search(path.name) is not None
            )
        )
    )
    assert not offenders, (
        'Found test Python files with unsupported naming patterns:\n- '
        + '\n- '.join(offenders)
    )
