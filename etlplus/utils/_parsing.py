"""
:mod:`etlplus.utils._parsing` module.

Shared tolerant parsing helpers for configuration-style payloads.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MappingFieldParser',
    'SequenceParser',
    'ValueParser',
]


# SECTION: CLASSES ========================================================== #


class ValueParser:
    """
    Centralize tolerant scalar parsing rules for config-like payloads.

    Notes
    -----
    - Optional string fields coerce non-``None`` values to strings.
    """

    @classmethod
    def optional_str(
        cls,
        value: object,
    ) -> str | None:
        """Return an optional string, coercing non-``None`` values."""
        if value is None:
            return None
        return value if isinstance(value, str) else str(value)


class MappingFieldParser(ValueParser):
    """
    Parse required fields from mapping-style payloads.

    Notes
    -----
    - Mapping validation belongs in :mod:`etlplus.utils._mapping`.
    - This class only extracts typed fields once a mapping is already known.
    """

    @classmethod
    def required_str(
        cls,
        data: Mapping[str, object],
        key: str,
    ) -> str | None:
        """Return one required string field from a mapping payload."""
        value = data.get(key)
        return value if isinstance(value, str) else None


class SequenceParser(ValueParser):
    """Parse sequence-style payloads into concrete sequence types."""

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
