# etlplus.api module.

Focused documentation for the `etlplus.api` subpackage: a lightweight HTTP client and helpers for paginated REST endpoints.

- Provides a small `EndpointClient` for calling JSON APIs
- Supports page-based and cursor-based pagination via `PaginationConfig`
- Simple bearer-auth credentials via `EndpointCredentialsBearer`
- Convenience helpers to extract records from nested JSON payloads

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
)

client = EndpointClient(
    base_url="https://api.example.com/v1",
    endpoints={
        "list": "/items",  # you can add more named endpoints here
    },
    # Optional: auth
    credentials=EndpointCredentialsBearer(token="<YOUR_TOKEN>")
)

# Page-based pagination
pg: PaginationConfig = {"type": "page", "page_size": 100}
rows = client.paginate("list", pagination=pg)
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
from etlplus.api import EndpointClient, PaginationConfig

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

rows = client.paginate("list", pagination=pg)
for row in rows:
    process(row)
```

## Authentication

Use bearer tokens with `EndpointCredentialsBearer`:

```python
from etlplus.api import EndpointClient, EndpointCredentialsBearer

client = EndpointClient(
    base_url="https://api.example.com/v1",
    endpoints={"list": "/items"},
    credentials=EndpointCredentialsBearer(token="<YOUR_TOKEN>")
)
```

## Errors and rate limiting

- Errors: See `etlplus/api/errors.py` for the concrete exceptions raised by the client.
- Rate limiting: See `etlplus/api/rate.py` for strategies used to throttle and retry.

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
