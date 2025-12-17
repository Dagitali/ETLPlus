# etlplus.api module.

Focused documentation for the `etlplus.api` subpackage: a lightweight HTTP client and helpers for
paginated REST endpoints.

- Provides a small `EndpointClient` for calling JSON APIs
- Supports page-, offset-, and cursor-based pagination via `PaginationConfig`
- Simple bearer-auth credentials via `EndpointCredentialsBearer`
- Convenience helpers to extract records from nested JSON payloads
- Returns the shared `JSONRecords` alias (a list of `JSONDict`) for paginated responses, matching
  the rest of the library.

Back to project overview: see the top-level [README](../../README.md).

## Installation

`etlplus.api` ships as part of the `etlplus` package. Install the package as usual:

```bash
pip install etlplus
# or for development
pip install -e ".[dev]"
```

## Quickstart

```python
import requests
from etlplus.api import (
  EndpointClient,
  EndpointCredentialsBearer,
  JSONRecords,
)

auth = EndpointCredentialsBearer(
  token_url="https://auth.example.com/oauth2/token",
  client_id="CLIENT_ID",
  client_secret="CLIENT_SECRET",
  scope="read:items",
)

session = requests.Session()
session.auth = auth

client = EndpointClient(
  base_url="https://api.example.com/v1",
  endpoints={
    "list": "/items",  # you can add more named endpoints here
  },
  retry={"max_attempts": 4, "backoff": 0.5},
  retry_network_errors=True,
  session=session,
)

# Page-based pagination
pg: PaginationConfig = {"type": "page", "page_size": 100}
rows: JSONRecords = client.paginate("list", pagination=pg)
for row in rows:
  print(row)
```

### Overriding rate limits per call

When a client is constructed with ``rate_limit`` metadata you can still tweak the pacing for
individual calls by passing ``rate_limit_overrides`` to ``paginate``/``paginate_iter``. The
overrides share the same shape as the base configuration and take precedence over the client
defaults.

```python
client = EndpointClient(
  base_url="https://api.example.com/v1",
  endpoints={"list": "/items"},
  rate_limit={"max_per_sec": 2},  # ~0.5s between calls when unspecified
)

rows = client.paginate(
  "list",
  pagination={"type": "page", "page_size": 100},
  rate_limit_overrides={"sleep_seconds": 0.1},  # per-call override
)
```

Precedence is ``overrides.sleep_seconds`` > ``overrides.max_per_sec`` > the same keys from
``client.rate_limit``. When no override is supplied the base settings are used.

## Choosing `records_path` and `cursor_path`

If the API responds like this:

```json
{
  "data": {
    "items": [{"id": 1}, {"id": 2}],
    "nextCursor": "abc123"
  }
}
```

- `records_path` should be `data.items`
- `cursor_path` should be `data.nextCursor`

If the response is a list at the top level, you can omit `records_path`.

## Cursor-based pagination example

```python
from etlplus.api import EndpointClient, PaginationConfig, JSONRecords

client = EndpointClient(
    base_url="https://api.example.com/v1",
    endpoints={"list": "/items"},
)

pg: PaginationConfig = {
    "type": "cursor",
    # Where records live in the JSON payload (dot path or top-level key)
    "records_path": "data.items",
    # Query parameter name that carries the cursor
    "cursor_param": "cursor",
    # Dot path in the response JSON that holds the next cursor value
    "cursor_path": "data.nextCursor",
    # Optional: limit per page
    "page_size": 100,
    # Optional: start from a specific cursor value
    # "start_cursor": "abc123",
}

rows: JSONRecords = client.paginate("list", pagination=pg)
for row in rows:
    process(row)
```

## Offset-based pagination example

```python
from etlplus.api import EndpointClient, PaginationConfig

client = EndpointClient(
    base_url="https://api.example.com/v1",
    endpoints={"list": "/items"},
)

pg: PaginationConfig = {
  "type": "offset",
  # Key holding the offset value on each request
  "page_param": "offset",
  # Key holding the page size (limit) on each request
  "size_param": "limit",
  # Starting offset (0 is common for offset-based APIs)
  "start_page": 0,
  # Number of records per page
  "page_size": 100,
  # Optional: where records live in the JSON payload
  # "records_path": "data.items",
  # Optional caps
  # "max_records": 1000,
}

rows = client.paginate("list", pagination=pg)
for row in rows:
    process(row)
```

## Authentication

Use bearer tokens with `EndpointCredentialsBearer` (OAuth2 client credentials flow). Attach it to a
`requests.Session` and pass that session to the client:

```python
import requests
from etlplus.api import EndpointClient, EndpointCredentialsBearer

auth = EndpointCredentialsBearer(
    token_url="https://auth.example.com/oauth2/token",
    client_id="CLIENT_ID",
    client_secret="CLIENT_SECRET",
    scope="read:items",
)

session = requests.Session()
session.auth = auth

client = EndpointClient(
    base_url="https://api.example.com/v1",
    endpoints={"list": "/items"},
    session=session,
)
```

`EndpointCredentialsBearer` refreshes tokens automatically, applies a 15-second default timeout
(`DEFAULT_TOKEN_TIMEOUT`), and omits the optional `scope` field when not provided so identity
providers can fall back to their own defaults. If you already possess a static token, attach it to a
`requests.Session` manually rather than instantiating `EndpointCredentialsBearer`.

## Errors and rate limiting

- Errors: `ApiRequestError`, `ApiAuthError`, and `PaginationError` (in `etlplus/api/errors.py`)
  include an `as_dict()` helper for structured logs.
- Rate limiting: `RateLimiter` and its `resolve_sleep_seconds` helper (in
  `etlplus/api/rate_limiter.py`) derives fixed sleeps or `max_per_sec` windows. The paginator now
  builds a `RateLimiter` whenever the effective delay comes from
  `rate_limit`/`rate_limit_overrides`, so each page fetch sleeps before making another HTTP call.
  Passing `rate_limit_overrides` to `paginate*` lets you momentarily speed up or slow down a single
  request without mutating the client-wide defaults.

## Types and transport

- Types: pagination config helpers live in `etlplus/api/paginator.py`; retry helpers (including
  `RetryPolicy`) live in `etlplus/api/retry_manager.py`; rate-limit helpers live in
  `etlplus/api/rate_limiter.py`. These are all re-exported from `etlplus.api` for convenience.
- Transport/session: `etlplus/api/transport.py` contains the HTTP adapter helpers and
  `etlplus/api/request_manager.py` wraps `requests` sessions plus retry orchestration. Advanced
  users may consult those modules to adapt behavior.

## Supporting modules

- `etlplus.api.types` collects friendly aliases such as `Headers`, `Params`, `Url`, and
  `RateLimitOverrides` (whose values accept numeric override inputs) so endpoint helpers share the
  same type vocabulary.
- `etlplus.utils` exposes lightweight helpers used across the project, including CLI-friendly
  functions like `json_type`/`print_json` plus numeric coercion utilities (`to_float`,
  `to_positive_int`, etc.).

## Minimal contract

- Inputs
  - `base_url: str`, `endpoints: dict[str, str]`
  - optional `credentials`
  - `pagination: PaginationConfig` for `paginate()`
- Outputs
  - `paginate(name, ...)` yields an iterator of JSON-like rows
- Errors
  - Network/HTTP errors raise exceptions; consult `errors.py`

## See also

- Top-level CLI and library usage in the main [README](../../README.md)
