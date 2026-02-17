"""
:mod:`tests.unit.meta.test_u_marker_coverage` module.

Guardrails for scope-marker coverage in test :mod:`conftest.py` modules.
"""

from __future__ import annotations

from pathlib import Path

# SECTION: INTERNAL CONSTANTS =============================================== #


_REPO_ROOT = Path(__file__).resolve().parents[3]
_TESTS_ROOT = _REPO_ROOT / 'tests'
_SCOPE_MARKERS = (
    ('unit', 'unit'),
    ('integration', 'integration'),
)


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _scope_conftests(scope_name: str) -> list[Path]:
    """Return sorted ``conftest.py`` paths for one test scope."""
    scope_root = _TESTS_ROOT / scope_name
    return sorted(scope_root.rglob('conftest.py'))


# SECTION: TESTS ============================================================ #


def test_integration_file_conftest_includes_smoke_marker() -> None:
    """Test file-integration scope preserving the smoke intent marker."""
    conftest_path = _TESTS_ROOT / 'integration' / 'file' / 'conftest.py'
    text = conftest_path.read_text(encoding='utf-8')
    assert 'pytest.mark.smoke' in text


def test_scope_conftests_declare_expected_scope_markers() -> None:
    """Test each scope ``conftest.py`` includes the expected scope marker."""
    for scope_name, marker_name in _SCOPE_MARKERS:
        conftests = _scope_conftests(scope_name)
        assert conftests, (
            f'No conftest.py files found under tests/{scope_name}'
        )
        missing = sorted(
            path.relative_to(_REPO_ROOT).as_posix()
            for path in conftests
            if f'pytest.mark.{marker_name}'
            not in path.read_text(
                encoding='utf-8',
            )
        )
        assert not missing, (
            f'Missing pytest.mark.{marker_name} in:\n- ' + '\n- '.join(missing)
        )
