"""
:mod:`etlplus.config` package.

Configuration models and helpers for ETLPlus.

This package defines models for data sources/targets ("connectors"), APIs,
pagination/rate limits, pipeline orchestration, and related utilities. The
parsers are permissive (accepting ``Mapping[str, Any]``) and normalize to
concrete types without raising on unknown/optional fields.

Notes
-----
- The models use ``@dataclass(slots=True)`` and avoid mutating inputs.
- TypedDicts are editor/type-checking hints and are not enforced at runtime.
"""

from __future__ import annotations

from .types import ApiConfigMap
from .types import ApiProfileConfigMap
from .types import ApiProfileDefaultsMap
from .types import EndpointMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Typed Dicts
    'ApiConfigMap',
    'ApiProfileConfigMap',
    'ApiProfileDefaultsMap',
    'EndpointMap',
]
