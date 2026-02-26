"""
:mod:`tests.unit.utils.test_u_utils_text` module.

Unit tests for :mod:`etlplus.utils.text`.
"""

from __future__ import annotations

from etlplus.utils import normalize_str

# SECTION: TESTS ============================================================ #


class TestText:
    """Unit tests for :mod:`etlplus.utils.text`."""

    def test_normalize_str(self) -> None:
        """Test that whitespace and casing are stripped."""
        assert normalize_str('  HeLLo  ') == 'hello'
        assert normalize_str(None) == ''
