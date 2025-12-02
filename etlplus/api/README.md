# etlplus.api module.

Focused documentation for the `etlplus.api` subpackage: a lightweight HTTP client and helpers for paginated REST endpoints.

- Provides a small `EndpointClient` for calling JSON APIs
- Supports page-, offset-, and cursor-based pagination via `PaginationConfig`
- Simple bearer-auth credentials via `EndpointCredentialsBearer`
- Convenience helpers to extract records from nested JSON payloads
- Returns the shared `JSONRecords` alias (a list of `JSONDict`) for paginated
  responses, matching the rest of the library.

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
from etlplus.api import (
    EndpointClient,
    EndpointCredentialsBearer,
    PaginationConfig,  # re-exported from etlplus.api.types
    JSONRecords,
)

client = EndpointClient(
    base_url="https://api.example.com/v1",
    endpoints={
        "list": "/items",  # you can add more named endpoints here
    },
    retry={"max_attempts": 4, "backoff": 0.5},
    retry_network_errors=True,
    # Optional: auth
    credentials=EndpointCredentialsBearer(token="<YOUR_TOKEN>")
    )

# Page-based pagination
pg: PaginationConfig = {"type": "page", "page_size": 100}
rows: JSONRecords = client.paginate("list", pagination=pg)
for row in rows:
    print(row)
```

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

Use bearer tokens with `EndpointCredentialsBearer` (OAuth2 client credentials
flow). Attach it to a `requests.Session` and pass that session to the client:

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

## Errors and rate limiting

- Errors: `ApiRequestError`, `ApiAuthError`, and `PaginationError` (in
  `etlplus/api/errors.py`) include an `as_dict()` helper for structured logs.
- Rate limiting: `RateLimiter` and `compute_sleep_seconds` (in
  `etlplus/api/request.py`) derive fixed sleeps or `max_per_sec` windows, and
  are used automatically when pagination requests specify rate-limit config.

## Types and transport

- Types: `etlplus/api/types.py` defines the `PaginationConfig` shape and other helper types. These are re-exported from `etlplus.api` for convenience.
- Transport: `etlplus/api/transport.py` contains the HTTP transport implementation. Advanced users may consult it to adapt behavior.

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
