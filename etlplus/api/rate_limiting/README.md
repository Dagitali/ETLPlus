# `etlplus.api.rate_limiting` Subpackage

Documentation for the `etlplus.api.rate_limiting` subpackage: small rate-limiting primitives for
HTTP request pacing.

- Resolves fixed sleep intervals and maximum requests-per-second settings
- Provides a lightweight `RateLimiter` runtime helper
- Shares override shapes used by `EndpointClient.paginate` and `paginate_iter`

Back to API overview: see [`etlplus.api`](../README.md).

- [Public API](#public-api)
- [Usage](#usage)
- [See Also](#see-also)

## Public API

The package facade exports:

- `RateLimitConfig`: immutable rate-limit configuration.
- `RateLimiter`: helper that sleeps according to resolved configuration.
- `RateLimitInput`, `RateLimitOverrides`, and `RateLimitConfigDict`: accepted configuration shapes.

## Usage

```python
from etlplus.api.rate_limiting import RateLimiter

limiter = RateLimiter.from_config({"max_per_sec": 2})
limiter.enforce()
```

For ordinary API calls, pass `rate_limit` to `EndpointClient` or `rate_limit_overrides` to a single
pagination call.

## See Also

- API package overview in [`../README.md`](../README.md)
- Pagination helpers in [`../pagination/README.md`](../pagination/README.md)
