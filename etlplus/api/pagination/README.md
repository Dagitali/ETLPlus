# `etlplus.api.pagination` Subpackage

Documentation for the `etlplus.api.pagination` subpackage: pagination configuration and runtime
helpers for REST API responses.

- Supports page-, offset-, and cursor-style pagination
- Provides configuration dataclasses and TypedDict shapes for API clients
- Exposes `Paginator` for advanced callers that need lower-level pagination control

Back to API overview: see [`etlplus.api`](../README.md).

- [Public API](#public-api)
- [Usage](#usage)
- [See Also](#see-also)

## Public API

Most callers use pagination through `EndpointClient`, but these helpers are also exported:

- `PaginationConfig`: normalized pagination configuration.
- `PaginationClient`: client-facing driver for traversing paginated responses.
- `Paginator`: lower-level paginator implementation.
- `PaginationType`: enum for supported pagination modes.
- `PaginationInput` and `PaginationConfigDict`: accepted configuration shapes.

## Usage

```python
from etlplus.api import EndpointClient

client = EndpointClient(
    base_url="https://api.example.com/v1",
    endpoints={"items": "/items"},
)
rows = client.paginate("items", pagination={"type": "page", "page_size": 100})
```

## See Also

- API package overview in [`../README.md`](../README.md)
- Rate limiting helpers in [`../rate_limiting/README.md`](../rate_limiting/README.md)
