"""
etlplus.api.__init__
====================

The top-level module defining ``:mod:etlplus.api``, a package of high-level
helpers for building simple REST API clients with pagination, retry, and
transport configuration.

Summary
-------
Use :class:`~etlplus.api.client.EndpointClient` to register relative endpoint
paths under a base URL and to paginate API responses. The client can apply
rate limits between requests and perform exponential-backoff retries with
full jitter.

Examples
--------

Page-based pagination
^^^^^^^^^^^^^^^^^^^^^
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
When you need to call an already-composed absolute URL, use
:meth:`EndpointClient.paginate_url`. It accepts the same pagination config
and returns either the raw JSON object (no pagination) or a list of record
dicts aggregated across pages.

Notes
-----
- ``EndpointClient.endpoints`` is read-only at runtime.
- Default pagination parameters are centralized on the client (e.g.,
    ``page``, ``per_page``, ``cursor``, ``limit``, start page ``1``, and
    page size ``100``). Override via the pagination config as needed.
- Retries are opt-in via the ``retry`` parameter. Backoff uses jitter to
    reduce thundering herds. Use ``retry_network_errors=True`` to also retry
    timeouts/connection errors.

See Also
--------
- :mod:`etlplus.api.types` for shared types such as
    ``PaginationConfig``, ``RetryPolicy``, and HTTP adapter configs
- :mod:`etlplus.api.transport` for HTTPAdapter helpers
"""
from __future__ import annotations

from .auth import EndpointCredentialsBearer
from .client import EndpointClient
from .rate import compute_sleep_seconds
from .transport import build_http_adapter
from .types import CursorPaginationConfig
from .types import HTTPAdapterMountConfig
from .types import HTTPAdapterRetryConfig
from .types import JSONData
from .types import JSONDict
from .types import JSONList
from .types import PagePaginationConfig
from .types import PaginationConfig
from .types import RateLimitConfig
from .types import RetryPolicy


# SECTION: PUBLIC API ======================================================= #


__all__ = [
    'EndpointClient',
    'EndpointCredentialsBearer',
    'build_http_adapter',
    'compute_sleep_seconds',

    # Common types
    'HTTPAdapterMountConfig', 'HTTPAdapterRetryConfig',
    'JSONDict', 'JSONList', 'JSONData', 'PaginationConfig',
    'CursorPaginationConfig', 'PagePaginationConfig',
    'RateLimitConfig', 'RetryPolicy',
]
