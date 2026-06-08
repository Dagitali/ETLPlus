"""
:mod:`tests.unit.test_u_api_enums` module.

Unit tests for :mod:`etlplus.utils._enums` coercion helpers and behaviors.
"""

from __future__ import annotations

import pytest

from etlplus.api import HttpMethod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestHttpMethod:
    """Unit tests for :class:`HttpMethod`."""

    @pytest.mark.parametrize(
        ('method', 'expected'),
        [
            pytest.param(HttpMethod.POST, True, id='post'),
            pytest.param(HttpMethod.PUT, True, id='put'),
            pytest.param(HttpMethod.PATCH, True, id='patch'),
            pytest.param(HttpMethod.GET, False, id='get'),
        ],
    )
    def test_allows_body(self, method: HttpMethod, expected: bool) -> None:
        """
        Test that the :meth:`allows_body` property reflects method semantics.
        """
        assert method.allows_body is expected

    def test_coerce(self) -> None:
        """Test that :meth:`coerce` resolves supported inputs."""
        assert HttpMethod.coerce('delete') is HttpMethod.DELETE
