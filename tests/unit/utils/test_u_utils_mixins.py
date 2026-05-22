"""
:mod:`tests.unit.utils.test_u_utils_mixins` module.

Unit tests for :mod:`etlplus.utils._mixins` helpers.
"""

from __future__ import annotations

import pytest

from etlplus.utils._mixins import BoundsWarningsMixin

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestBoundsWarningsMixin:
    """Unit tests for :class:`BoundsWarningsMixin`."""

    @pytest.mark.parametrize(
        ('calls', 'expected'),
        [
            pytest.param([(True, 'limit reached')], ['limit reached'], id='append'),
            pytest.param(
                [(True, 'first'), (True, 'second')],
                ['first', 'second'],
                id='reuse-bucket',
            ),
            pytest.param([(False, 'ignored')], [], id='skip'),
        ],
    )
    def test_warn_if(
        self,
        calls: list[tuple[bool, str]],
        expected: list[str],
    ) -> None:
        """Test that conditional warnings update the caller-provided bucket."""
        warnings: list[str] = []
        for condition, message in calls:
            BoundsWarningsMixin._warn_if(condition, message, warnings)

        assert warnings == expected
