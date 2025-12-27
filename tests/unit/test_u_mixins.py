"""
:mod:`tests.unit.test_u_mixins` module.

Unit tests for :mod:`etlplus.mixins` helpers.
"""

from __future__ import annotations

import pytest

from etlplus.mixins import BoundsWarningsMixin

# SECTION: HELPERS ========================================================== #

pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestBoundsWarningsMixin:
    """Unit tests for :class:`BoundsWarningsMixin`."""

    # pylint: disable=protected-access

    def test_warn_if_appends_message(self) -> None:
        """Test that warnings are appended when the condition is ``True``."""

        warnings: list[str] = []
        BoundsWarningsMixin._warn_if(True, 'limit reached', warnings)
        assert warnings == ['limit reached']

    def test_warn_if_reuses_bucket(self) -> None:
        """Test that multiple warnings reuse the same list."""

        warnings: list[str] = []
        BoundsWarningsMixin._warn_if(True, 'first', warnings)
        BoundsWarningsMixin._warn_if(True, 'second', warnings)
        assert warnings == ['first', 'second']

    def test_warn_if_skips_when_condition_false(self) -> None:
        """Test that no warnings are added when the condition is ``False``."""

        warnings: list[str] = []
        BoundsWarningsMixin._warn_if(False, 'ignored', warnings)
        assert not warnings
