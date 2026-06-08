"""
:mod:`tests.meta.test_m_marker_coverage` module.

Guardrails for scope-marker coverage in test :mod:`conftest.py` modules.
"""

from __future__ import annotations

import pytest

from tests.meta.pytest_meta_support import read_text
from tests.pytest_shared_support import REPO_ROOT
from tests.pytest_shared_support import TESTS_ROOT

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: INTERNAL CONSTANTS =============================================== #


_CONFTEST_MARKER_CASES = (
    pytest.param('meta', 'meta', id='meta'),
    pytest.param('unit', 'unit', id='unit'),
    pytest.param('integration', 'integration', id='integration'),
    pytest.param('integration/file', 'smoke', id='integration-file-smoke'),
)


# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    ('scope_path', 'marker_name'),
    _CONFTEST_MARKER_CASES,
)
def test_scope_conftests_declare_expected_scope_markers(
    scope_path: str,
    marker_name: str,
) -> None:
    """
    Test that each scope ``conftest.py`` includes its expected marker.
    """
    conftests = sorted((TESTS_ROOT / scope_path).rglob('conftest.py'))
    assert conftests, f'No conftest.py files found under tests/{scope_path}'
    missing = sorted(
        path.relative_to(REPO_ROOT).as_posix()
        for path in conftests
        if f'pytest.mark.{marker_name}' not in read_text(path)
    )
    assert not missing, f'Missing pytest.mark.{marker_name} in:\n- ' + '\n- '.join(
        missing,
    )
