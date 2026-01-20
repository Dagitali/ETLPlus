"""
:mod:`tests.unit.test_u_enums` module.

Unit tests for :mod:`etlplus.enums` coercion helpers and behaviors.
"""

from __future__ import annotations

import pytest

from etlplus.api import HttpMethod

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


class TestHttpMethod:
    """Unit test suite for :class:`etlplus.enums.HttpMethod`."""

    def test_allows_body(self) -> None:
        """Test the `allows_body` property."""
        assert HttpMethod.POST.allows_body is True
        assert HttpMethod.PUT.allows_body is True
        assert HttpMethod.PATCH.allows_body is True
        assert HttpMethod.GET.allows_body is False

    def test_coerce(self) -> None:
        """Test :meth:`coerce`."""
        assert HttpMethod.coerce('delete') is HttpMethod.DELETE
