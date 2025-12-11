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


type Headers = StrStrMap
type Params = StrAnyMap
type Url = str

type FetchPageCallable = Callable[
    [Url, Params | None, int | None],
    JSONData,
]

type RateLimitOverrideValue = float | int | None
type RateLimitOverrides = Mapping[str, RateLimitOverrideValue] | None
