"""
:mod:`tests.unit.database.test_u_database_enums` module.

Unit tests for :mod:`etlplus.database.enums`.
"""

from __future__ import annotations

from etlplus.database.enums import HttpMethod


class TestHttpMethod:
    """Unit tests for :class:`HttpMethod`."""

    def test_allows_body_for_mutating_payload_methods(self) -> None:
        """POST, PUT, and PATCH should report body support."""
        assert HttpMethod.POST.allows_body is True
        assert HttpMethod.PUT.allows_body is True
        assert HttpMethod.PATCH.allows_body is True

    def test_disallows_body_for_other_methods(self) -> None:
        """Other HTTP verbs should report no body support."""
        for method in (
            HttpMethod.CONNECT,
            HttpMethod.DELETE,
            HttpMethod.GET,
            HttpMethod.HEAD,
            HttpMethod.OPTIONS,
            HttpMethod.TRACE,
        ):
            assert method.allows_body is False
