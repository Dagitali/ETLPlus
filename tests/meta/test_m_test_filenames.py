"""
:mod:`tests.meta.test_m_test_filenames` module.

Guardrails for test-module filename conventions.
"""

from __future__ import annotations

import re

from tests.pytest_shared_support import REPO_ROOT
from tests.pytest_shared_support import TESTS_ROOT

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: INTERNAL CONSTANTS =============================================== #


_DUPLICATE_SUFFIX_PATTERN = re.compile(r'\s+\d+\.py$')


# SECTION: TESTS ============================================================ #


def test_python_test_filenames_have_no_spaces_or_numbered_duplicates() -> None:
    """
    Test that test-related Python filenames avoid spaces and ``" 2.py"``-style
    duplicate suffixes.
    """
    offenders = sorted(
        path.relative_to(REPO_ROOT).as_posix()
        for path in TESTS_ROOT.rglob('*.py')
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
