"""
:mod:`etlplus.utils._mapping` module.

Mapping-oriented utility helpers.
"""

from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Set
from typing import Any
from typing import TypeVar

from ._types import StrAnyMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MappingParser',
]


# SECTION: TYPE VARIABLES =================================================== #


ItemT = TypeVar('ItemT')


# SECTION: CLASSES ========================================================== #


class MappingParser:
    """Normalize optionally mapping-like inputs into concrete mapping shapes."""

    # -- Static Methods -- #

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
            if not isinstance(raw_name := getattr(item, 'name', None), str):
                continue
            name = raw_name.strip()
            if not name:
                continue
            if name in indexed:
                raise ValueError(f'Duplicate {item_label} name: {name}')
            indexed[name] = item
        return indexed

    @staticmethod
    def merge_to_dict(
        *mapping_sets: object,
        excluded_keys: Set[str] = frozenset(),
    ) -> dict[str, Any]:
        """
        Merge mapping-like values with later mappings taking precedence.

        Parameters
        ----------
        *mapping_sets : object
            Any number of mapping-like values to merge. Non-mappings are
            ignored.
        excluded_keys : Set[str], optional
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
        value: Any,
    ) -> dict[str, Any]:
        """
        Return a ``dict`` copy when *value* is mapping-like.

        Parameters
        ----------
        value : Any
            Mapping-like object to copy. ``None`` returns an empty dict.

        Returns
        -------
        dict[str, Any]
            Shallow copy of *value* converted to a standard ``dict``.
        """
        return dict(value) if isinstance(value, Mapping) else {}

    @staticmethod
    def optional(
        value: Any,
    ) -> StrAnyMap | None:
        """
        Return *value* when it is mapping-like; otherwise ``None``.

        Parameters
        ----------
        value : Any
            Value to test.

        Returns
        -------
        StrAnyMap | None
            The input value if it is a mapping; ``None`` if not.
        """
        return value if isinstance(value, Mapping) else None
