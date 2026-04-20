"""
:mod:`etlplus.utils._parsing` module.

Shared tolerant parsing helpers for configuration-style payloads.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence

from ._mapping import maybe_mapping
from ._types import StrAnyMap


class ValueParser:
    """
    Centralize tolerant parsing rules for config-like payloads.

    Notes
    -----
    - Non-mapping inputs are treated as invalid payloads.
    - Optional string fields coerce non-``None`` values to strings.
    - Sequence fields preserve only string entries.
    """

    @classmethod
    def mapping(
        cls,
        obj: object,
    ) -> StrAnyMap | None:
        """Return a mapping payload when *obj* is mapping-like."""
        return maybe_mapping(obj)

    @classmethod
    def optional_str(
        cls,
        value: object,
    ) -> str | None:
        """Return an optional string, coercing non-``None`` values."""
        if value is None:
            return None
        return value if isinstance(value, str) else str(value)

    @classmethod
    def required_str(
        cls,
        data: Mapping[str, object],
        key: str,
    ) -> str | None:
        """Return one required string field from a mapping payload."""
        value = data.get(key)
        return value if isinstance(value, str) else None

    @classmethod
    def str_list(
        cls,
        value: object,
    ) -> list[str]:
        """Normalize a string or a sequence of strings into a string list."""
        match value:
            case str():
                return [value]
            case Sequence() if not isinstance(value, str | bytes | bytearray):
                return [entry for entry in value if isinstance(entry, str)]
            case _:
                return []
