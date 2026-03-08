"""
:mod:`tests.unit.test_u_api_enums` module.

Unit tests for :mod:`etlplus.utils.enums` coercion helpers and behaviors.
"""

from __future__ import annotations

from etlplus.api import HttpMethod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestHttpMethod:
    """Unit tests for :class:`HttpMethod`."""

    def test_allows_body(self) -> None:
        """
        Test that the :meth:`allows_body` property reflects method semantics.
        """
        assert HttpMethod.POST.allows_body is True
        assert HttpMethod.PUT.allows_body is True
        assert HttpMethod.PATCH.allows_body is True
        assert HttpMethod.GET.allows_body is False

    def test_coerce(self) -> None:
        """Test that :meth:`coerce` resolves supported inputs."""
        assert HttpMethod.coerce('delete') is HttpMethod.DELETE
