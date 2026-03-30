"""
:mod:`tests.unit.api.test_u_api_init` module.

Unit tests for :mod:`etlplus.api` package exports.
"""

from __future__ import annotations

import etlplus.api as api_pkg
from etlplus.api.pagination import PaginationInput
from etlplus.api.rate_limiting import RateLimitOverrides

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestApiPackageExports:
    """Unit tests for top-level API package exports."""

    def test_expected_symbols_are_exported(self) -> None:
        """Test that the API package exposes documented stable helpers."""
        expected = {
            'EndpointClient',
            'PaginationConfig',
            'PaginationInput',
            'PaginationType',
            'Paginator',
            'RateLimitConfig',
            'RateLimitOverrides',
            'RateLimiter',
            'RetryManager',
        }
        assert expected.issubset(set(api_pkg.__all__))

    def test_new_type_alias_exports_resolve_to_subpackage_symbols(self) -> None:
        """Test that package-level type aliases point at canonical subpackages."""
        assert api_pkg.PaginationInput is PaginationInput
        assert api_pkg.RateLimitOverrides is RateLimitOverrides
