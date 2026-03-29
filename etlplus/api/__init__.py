"""
:mod:`etlplus.api` package.

High-level helpers for building REST API clients with pagination, retry,
rate limiting, and transport configuration.

Summary
-------
Use :class:`etlplus.api.EndpointClient` to register relative endpoint paths
under a base URL and paginate responses. The client can apply rate limits
between requests and perform exponential-backoff retries with full jitter.

Examples
--------
Page-based pagination
^^^^^^^^^^^^^^^^^^^^^
>>> from etlplus.api import EndpointClient
>>> client = EndpointClient(
...     base_url="https://api.example.com/v1",
...     endpoints={"list_users": "/users"},
... )
>>> page_cfg = {
...     "type": "page",               # or "offset"
...     "records_path": "data.items", # dotted path into payload
...     "page_param": "page",
...     "size_param": "per_page",
...     "start_page": 1,
...     "page_size": 100,
... }
>>> rows = client.paginate(
...     "list_users",
...     query_parameters={"active": "true"},
...     pagination=page_cfg,
... )

Retries and network errors
^^^^^^^^^^^^^^^^^^^^^^^^^^
>>> client = EndpointClient(
...     base_url="https://api.example.com/v1",
...     endpoints={"list": "/items"},
...     retry={"max_attempts": 5, "backoff": 0.5, "retry_on": [429, 503]},
...     retry_network_errors=True,
... )
>>> items = client.paginate(
...     "list", pagination={"type": "page", "page_size": 50}
... )

Absolute URLs
^^^^^^^^^^^^^
Use :meth:`EndpointClient.paginate_url` for an already composed absolute URL.
It accepts the same pagination config and returns either the raw JSON object
(no pagination) or a list of record dicts aggregated across pages.

Notes
-----
- ``EndpointClient.endpoints`` is read-only at runtime.
- Pagination defaults are centralized on the client (``page``, ``per_page``,
    ``cursor``, ``limit``; start page ``1``; page size ``100``).
- Retries are opt-in via the ``retry`` parameter; backoff uses jitter.
- Use ``retry_network_errors=True`` to also retry timeouts/connection errors.
- Prefer :data:`JSONRecords` (list of :data:`JSONDict`) for paginated
    responses; scalar/record aliases are exported for convenience.
- The underlying :class:`Paginator` is exported for advanced scenarios that
    need to stream pages manually.

See Also
--------
- :mod:`etlplus.api.pagination` for pagination helpers and config shapes
- :mod:`etlplus.api.rate_limiting` for rate-limit helpers and config shapes
- :mod:`etlplus.api._errors` for API error exceptions
- :mod:`etlplus.api._retry_manager` for retry policies
- :mod:`etlplus.api._transport` for HTTPAdapter helpers
- retry, error, and transport helpers re-exported by :mod:`etlplus.api`
"""

from __future__ import annotations

from ._auth import EndpointCredentialsBearer
from ._config import ApiConfig
from ._config import ApiProfileConfig
from ._config import EndpointConfig
from ._enums import HttpMethod
from ._errors import ApiAuthError
from ._errors import ApiRequestError
from ._errors import PaginationError
from ._retry_manager import RetryManager
from ._retry_manager import RetryPolicyDict
from ._retry_manager import RetryStrategy
from ._transport import HTTPAdapterMountConfigDict
from ._transport import HTTPAdapterRetryConfigDict
from ._transport import build_http_adapter
from ._transport import build_session_with_adapters
from ._types import ApiConfigDict
from ._types import ApiProfileConfigDict
from ._types import ApiProfileDefaultsDict
from ._types import EndpointConfigDict
from ._types import FetchPageCallable
from ._types import Headers
from ._types import Params
from ._types import RequestOptions
from ._types import Url
from ._utils import compose_api_request_env
from ._utils import compose_api_target_env
from ._utils import paginate_with_client
from ._utils import resolve_request
from .endpoint_client import EndpointClient
from .pagination import CursorPaginationConfigDict
from .pagination import PagePaginationConfigDict
from .pagination import PaginationClient
from .pagination import PaginationConfig
from .pagination import PaginationConfigDict
from .pagination import PaginationInput
from .pagination import PaginationType
from .pagination import Paginator
from .rate_limiting import RateLimitConfig
from .rate_limiting import RateLimitConfigDict
from .rate_limiting import RateLimiter
from .rate_limiting import RateLimitOverrides

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'EndpointClient',
    'EndpointCredentialsBearer',
    'Paginator',
    'RateLimiter',
    'RetryManager',
    # Exceptions
    'ApiAuthError',
    'ApiRequestError',
    'PaginationError',
    # Data Classes
    'ApiConfig',
    'ApiProfileConfig',
    'EndpointConfig',
    'PaginationClient',
    'PaginationConfig',
    'RateLimitConfig',
    'RequestOptions',
    'RetryStrategy',
    # Enums
    'HttpMethod',
    'PaginationType',
    # Functions
    'build_http_adapter',
    'build_session_with_adapters',
    'compose_api_request_env',
    'compose_api_target_env',
    'paginate_with_client',
    'resolve_request',
    # Type Aliases
    'ApiConfigDict',
    'ApiProfileConfigDict',
    'ApiProfileDefaultsDict',
    'CursorPaginationConfigDict',
    'EndpointConfigDict',
    'FetchPageCallable',
    'Headers',
    'HTTPAdapterMountConfigDict',
    'HTTPAdapterRetryConfigDict',
    'PagePaginationConfigDict',
    'PaginationConfigDict',
    'PaginationInput',
    'Params',
    'RateLimitConfigDict',
    'RateLimitOverrides',
    'RetryPolicyDict',
    'Url',
]
