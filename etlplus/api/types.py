"""
:mod:`etlplus.api.types` module.

Thin, centralized HTTP-oriented type aliases for the
:mod:`etlplus.api` package.

Examples
--------
>>> from etlplus.api import Url
>>> url: Url = "https://api.example.com/data"
>>> from etlplus.api import Headers
>>> headers: Headers = {"Authorization": "Bearer token"}
>>> from etlplus.api import Params
>>> params: Params = {"query": "search term", "limit": 50}
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # type Aliases
    'Headers',
    'Params',
    'Url',
]


# SECTION: TYPE ALIASES ===================================================== #


type Headers = Mapping[str, str]
type Params = Mapping[str, Any]
type Url = str
