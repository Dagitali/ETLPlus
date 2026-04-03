"""
:mod:`tests.unit.api.test_u_api_init` module.

Unit tests for :mod:`etlplus.api` package facade exports.
"""

from __future__ import annotations

import pytest

import etlplus.api as api_pkg
from etlplus.api._auth import EndpointCredentialsBearer
from etlplus.api._config import ApiConfig
from etlplus.api._config import ApiProfileConfig
from etlplus.api._config import EndpointConfig
from etlplus.api._enums import HttpMethod
from etlplus.api._errors import ApiAuthError
from etlplus.api._errors import ApiRequestError
from etlplus.api._errors import PaginationError
from etlplus.api._retry_manager import RetryManager
from etlplus.api._retry_manager import RetryPolicyDict
from etlplus.api._retry_manager import RetryStrategy
from etlplus.api._transport import HTTPAdapterMountConfigDict
from etlplus.api._transport import HTTPAdapterRetryConfigDict
from etlplus.api._transport import build_http_adapter
from etlplus.api._transport import build_session_with_adapters
from etlplus.api._types import ApiConfigDict
from etlplus.api._types import ApiProfileConfigDict
from etlplus.api._types import ApiProfileDefaultsDict
from etlplus.api._types import EndpointConfigDict
from etlplus.api._types import FetchPageCallable
from etlplus.api._types import Headers
from etlplus.api._types import Params
from etlplus.api._types import RequestOptions
from etlplus.api._types import Url
from etlplus.api._utils import compose_api_request_env
from etlplus.api._utils import compose_api_target_env
from etlplus.api._utils import paginate_with_client
from etlplus.api._utils import resolve_request
from etlplus.api.endpoint_client import EndpointClient
from etlplus.api.pagination import CursorPaginationConfigDict
from etlplus.api.pagination import PagePaginationConfigDict
from etlplus.api.pagination import PaginationClient
from etlplus.api.pagination import PaginationConfig
from etlplus.api.pagination import PaginationConfigDict
from etlplus.api.pagination import PaginationInput
from etlplus.api.pagination import PaginationType
from etlplus.api.pagination import Paginator
from etlplus.api.rate_limiting import RateLimitConfig
from etlplus.api.rate_limiting import RateLimitConfigDict
from etlplus.api.rate_limiting import RateLimiter
from etlplus.api.rate_limiting import RateLimitOverrides

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


API_EXPORTS = [
    ('EndpointClient', EndpointClient),
    ('EndpointCredentialsBearer', EndpointCredentialsBearer),
    ('Paginator', Paginator),
    ('RateLimiter', RateLimiter),
    ('RetryManager', RetryManager),
    ('ApiAuthError', ApiAuthError),
    ('ApiRequestError', ApiRequestError),
    ('PaginationError', PaginationError),
    ('ApiConfig', ApiConfig),
    ('ApiProfileConfig', ApiProfileConfig),
    ('EndpointConfig', EndpointConfig),
    ('PaginationClient', PaginationClient),
    ('PaginationConfig', PaginationConfig),
    ('RateLimitConfig', RateLimitConfig),
    ('RequestOptions', RequestOptions),
    ('RetryStrategy', RetryStrategy),
    ('HttpMethod', HttpMethod),
    ('PaginationType', PaginationType),
    ('build_http_adapter', build_http_adapter),
    ('build_session_with_adapters', build_session_with_adapters),
    ('compose_api_request_env', compose_api_request_env),
    ('compose_api_target_env', compose_api_target_env),
    ('paginate_with_client', paginate_with_client),
    ('resolve_request', resolve_request),
    ('ApiConfigDict', ApiConfigDict),
    ('ApiProfileConfigDict', ApiProfileConfigDict),
    ('ApiProfileDefaultsDict', ApiProfileDefaultsDict),
    ('CursorPaginationConfigDict', CursorPaginationConfigDict),
    ('EndpointConfigDict', EndpointConfigDict),
    ('FetchPageCallable', FetchPageCallable),
    ('Headers', Headers),
    ('HTTPAdapterMountConfigDict', HTTPAdapterMountConfigDict),
    ('HTTPAdapterRetryConfigDict', HTTPAdapterRetryConfigDict),
    ('PagePaginationConfigDict', PagePaginationConfigDict),
    ('PaginationConfigDict', PaginationConfigDict),
    ('PaginationInput', PaginationInput),
    ('Params', Params),
    ('RateLimitConfigDict', RateLimitConfigDict),
    ('RateLimitOverrides', RateLimitOverrides),
    ('RetryPolicyDict', RetryPolicyDict),
    ('Url', Url),
]

# SECTION: TESTS ============================================================ #


class TestApiPackageExports:
    """Unit tests for package-level exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert api_pkg.__all__ == [name for name, _value in API_EXPORTS]

    @pytest.mark.parametrize(('name', 'expected'), API_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(api_pkg, name) == expected
