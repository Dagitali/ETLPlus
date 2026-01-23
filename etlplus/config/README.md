# `etlplus.config` Subpackage

Documentation for the `etlplus.config` subpackage: type definitions and config shape helpers for
ETLPlus.

- Exposes TypedDict-based config schemas for API profiles and endpoints
- Provides exported type aliases for API configuration maps
- Designed for Python 3.13 typing and editor assistance (runtime parsing lives elsewhere)

Back to project overview: see the top-level [README](../../README.md).

- [`etlplus.config` Subpackage](#etlplusconfig-subpackage)
  - [Modules](#modules)
  - [Exported Types](#exported-types)
  - [Example: Typing an API Config](#example-typing-an-api-config)
  - [See Also](#see-also)

## Modules

- `etlplus.config.__init__`: package exports and high-level package notes
- `etlplus.config.types`: TypedDict-based config schemas

## Exported Types

- `ApiConfigMap`: top-level API config shape
- `ApiProfileConfigMap`: per-profile API config shape
- `ApiProfileDefaultsMap`: defaults block within a profile
- `EndpointMap`: endpoint config shape

## Example: Typing an API Config

```python
from etlplus.config import ApiConfigMap

api_cfg: ApiConfigMap = {
    "base_url": "https://example.test",
    "headers": {"Authorization": "Bearer token"},
    "endpoints": {
        "users": {
            "path": "/users",
            "method": "GET",
        },
    },
}
```

## See Also

- Top-level CLI and library usage in the main [README](../../README.md)
- Config type definitions in [types.py](types.py)
