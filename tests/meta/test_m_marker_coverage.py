"""
:mod:`tests.meta.test_m_marker_coverage` module.

Guardrails for scope-marker coverage in test :mod:`conftest.py` modules.
"""

from __future__ import annotations

import pytest

from tests.meta.pytest_meta_support import REPO_ROOT
from tests.meta.pytest_meta_support import TESTS_ROOT

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: INTERNAL CONSTANTS =============================================== #


_SCOPE_MARKERS = (
    pytest.param('meta', 'meta', id='meta'),
    pytest.param('unit', 'unit', id='unit'),
    pytest.param('integration', 'integration', id='integration'),
)


# SECTION: TESTS ============================================================ #


def test_integration_file_conftest_includes_smoke_marker() -> None:
    """Test that file-integration scope preserves the smoke intent marker."""
    conftest_path = TESTS_ROOT / 'integration' / 'file' / 'conftest.py'
    assert 'pytest.mark.smoke' in conftest_path.read_text(encoding='utf-8')


@pytest.mark.parametrize(
    ('scope_name', 'marker_name'),
    _SCOPE_MARKERS,
)
def test_scope_conftests_declare_expected_scope_markers(
    scope_name: str,
    marker_name: str,
) -> None:
    """
    Test that each scope ``conftest.py`` includes the expected scope marker.
    """
    conftests = sorted((TESTS_ROOT / scope_name).rglob('conftest.py'))
    assert conftests, f'No conftest.py files found under tests/{scope_name}'
    missing = sorted(
        path.relative_to(REPO_ROOT).as_posix()
        for path in conftests
        if f'pytest.mark.{marker_name}'
        not in path.read_text(
            encoding='utf-8',
        )
    )
    assert not missing, f'Missing pytest.mark.{marker_name} in:\n- ' + '\n- '.join(
        missing,
    )
