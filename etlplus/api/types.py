"""
:mod:`etlplus.api.types` module.

HTTP-centric type aliases for :mod:`etlplus.api` helpers.

Notes
-----
- Keeps pagination, transport, and higher-level modules decoupled from
    ``typing`` details.
- Uses ``Mapping`` inputs to accept both ``dict`` and mapping-like objects.

Examples
--------
>>> from etlplus.api import Url, Headers, Params
>>> url: Url = 'https://api.example.com/data'
>>> headers: Headers = {'Authorization': 'Bearer token'}
>>> params: Params = {'query': 'search term', 'limit': 50}
"""
from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping

from ..types import JSONData
from ..types import StrAnyMap
from ..types import StrStrMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Type Aliases
    'FetchPageCallable',
    'Headers',
    'Params',
    'RateLimitOverrides',
    'Url',
]


# SECTION: TYPE ALIASES ===================================================== #


# HTTP headers represented as a string-to-string mapping.
type Headers = StrStrMap

# Query or body parameters allowing arbitrary JSON-friendly values.
type Params = StrAnyMap

# Fully qualified resource locator consumed by transport helpers.
type Url = str

# Callable signature used by pagination helpers to fetch data pages.
type FetchPageCallable = Callable[
    [Url, Params | None, int | None],
    JSONData,
]

# Value accepted by rate-limit override mappings (seconds or counts).
type RateLimitOverrideValue = float | int | None

# Optional mapping of rate-limit fields to override values.
type RateLimitOverrides = Mapping[str, RateLimitOverrideValue] | None
