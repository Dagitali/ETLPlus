"""
:mod:`etlplus.utils._mapping` module.

Mapping-oriented utility helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any
from typing import TypeVar

from ._types import StrAnyMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MappingParser',
]


# SECTION: TYPE VARS ======================================================== #


ItemT = TypeVar('ItemT')


# SECTION: CLASSES ========================================================== #


class MappingParser:
    """Normalize optionally mapping-like inputs into concrete mapping shapes."""

    # -- Internal Static Methods -- #

    @staticmethod
    def _item_name(
        item: object,
    ) -> str | None:
        """Return one usable item name derived from a ``name`` attribute."""
        return MappingParser._non_empty_str(getattr(item, 'name', None))

    @staticmethod
    def _non_empty_str(
        value: object,
    ) -> str | None:
        """Return one stripped non-empty string value when available."""
        return value.strip() if isinstance(value, str) and value.strip() else None

    # -- Static Methods -- #

    @staticmethod
    def first_non_empty_str(
        mapping: StrAnyMap,
        keys: Iterable[str],
        *,
        nested_key: str | None = None,
    ) -> str | None:
        """
        Return the first non-empty string found for candidate keys.

        Parameters
        ----------
        mapping : StrAnyMap
            Mapping to inspect.
        keys : Iterable[str]
            Candidate keys checked in order.
        nested_key : str | None, optional
            Optional nested mapping key to inspect recursively after the
            top-level keys are checked.

        Returns
        -------
        str | None
            Trimmed string value if found; otherwise ``None``.
        """
        candidates = tuple(keys)
        current_mapping: Mapping[str, object] | None = mapping

        while current_mapping is not None:
            for key in candidates:
                if (
                    text := MappingParser._non_empty_str(current_mapping.get(key))
                ) is not None:
                    return text

            if nested_key is None:
                break
            nested = current_mapping.get(nested_key)
            current_mapping = nested if isinstance(nested, Mapping) else None

        return None

    @staticmethod
    def freeze(
        mapping: Mapping[Any, Any],
        *,
        key_cast: Callable[[Any], Any] | None = None,
    ) -> MappingProxyType:
        """
        Return an immutable copy of a mapping, optionally normalizing keys.

        Parameters
        ----------
        mapping : Mapping[Any, Any]
            Source mapping to freeze.
        key_cast : Callable[[Any], Any] | None, optional
            Optional key coercion applied to each key.

        Returns
        -------
        MappingProxyType
            Read-only mapping proxy with normalized keys.
        """
        data = {
            key if key_cast is None else key_cast(key): value
            for key, value in mapping.items()
        }
        return MappingProxyType(data)

    @staticmethod
    def index_named_items(
        items: Iterable[ItemT],
        *,
        item_label: str,
    ) -> dict[str, ItemT]:
        """
        Index named items and reject duplicates with a descriptive error.

        Parameters
        ----------
        items : Iterable[ItemT]
            Items to index. Only entries with a non-empty string ``name``
            attribute are included.
        item_label : str
            Human-readable label used in duplicate-name error messages.

        Returns
        -------
        dict[str, ItemT]
            Mapping of item names to their corresponding objects.

        Raises
        ------
        ValueError
            If duplicate names are found.
        """
        indexed: dict[str, ItemT] = {}
        for item in items:
            if (name := MappingParser._item_name(item)) is None:
                continue
            if name in indexed:
                raise ValueError(f'Duplicate {item_label} name: {name}')
            indexed[name] = item
        return indexed

    @staticmethod
    def merge_to_dict(
        *mapping_sets: object,
        excluded_keys: Iterable[str] = (),
    ) -> dict[str, Any]:
        """
        Merge mapping-like values with later mappings taking precedence.

        Parameters
        ----------
        *mapping_sets : object
            Any number of mapping-like values to merge. Non-mappings are
            ignored.
        excluded_keys : Iterable[str], optional
            Keys to remove from the merged result after merging.

        Returns
        -------
        dict[str, Any]
            Dictionary containing the merged key-value pairs.
        """
        merged: dict[str, Any] = {}
        for mapping_set in mapping_sets:
            if isinstance(mapping_set, Mapping):
                merged.update(mapping_set)
        for key in excluded_keys:
            merged.pop(key, None)
        return merged

    @staticmethod
    def to_str_dict(
        mapping: StrAnyMap | None,
    ) -> dict[str, str]:
        """
        Return a new ``dict`` with keys and values coerced to ``str``.

        Parameters
        ----------
        mapping : StrAnyMap | None
            Mapping to normalize; ``None`` yields ``{}``.

        Returns
        -------
        dict[str, str]
            Dictionary of the original key/value pairs converted via :func:`str`.
        """
        if not mapping:
            return {}
        return {str(key): str(value) for key, value in mapping.items()}

    @staticmethod
    def to_dict(
        value: object,
    ) -> dict[str, Any]:
        """
        Return a ``dict`` copy when *value* is mapping-like.

        Parameters
        ----------
        value : object
            Mapping-like object to copy. ``None`` returns an empty dict.

        Returns
        -------
        dict[str, Any]
            Shallow copy of *value* converted to a standard ``dict``.
        """
        if (mapping := MappingParser.optional(value)) is None:
            return {}
        return dict(mapping)

    @staticmethod
    def optional(
        value: object,
    ) -> StrAnyMap | None:
        """
        Return *value* when it is mapping-like; otherwise ``None``.

        Parameters
        ----------
        value : object
            Value to test.

        Returns
        -------
        StrAnyMap | None
            The input value if it is a mapping; ``None`` if not.
        """
        return value if isinstance(value, Mapping) else None
