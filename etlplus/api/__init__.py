"""
ETLPlus API
===========

Helpers for building simple REST API clients (pagination, request utils).

Primary usage
-------------

Prefer using :meth:`EndpointClient.paginate` with an endpoint key registered
on the client. This builds the absolute URL for you and aggregates records
across pages when a pagination config is provided.

Example
~~~~~~~

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

Escape hatch: absolute URLs
---------------------------

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
"""
from __future__ import annotations

from .auth import EndpointCredentialsBearer
from .client import EndpointClient
from .request import compute_sleep_seconds


# SECTION: PUBLIC API ======================================================= #


__all__ = [
    'EndpointCredentialsBearer',
    'EndpointClient',
    'compute_sleep_seconds',
]
