"""
:mod:`etlplus.ops._mappings` module.

Helpers for indexing named objects and merging mapping-style options.
"""

from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'index_named_items',
    'merge_mapping_options',
]


# SECTION: FUNCTIONS ======================================================== #


def index_named_items[ItemT](
    items: Iterable[ItemT],
    *,
    item_label: str,
) -> dict[str, ItemT]:
    """
    Index named items and reject duplicates with a descriptive error.

    Items without a non-empty string ``name`` attribute are ignored.

    Parameters
    ----------
    items : Iterable[ItemT]
        An iterable of items to index. Each item must have a ``name`` attribute
        that is a non-empty string to be included in the index.
    item_label : str
        A label for the type of items being indexed, used in error messages.

    Returns
    -------
    dict[str, ItemT]
        A dictionary mapping item names to their corresponding items. Only
        items with a valid non-empty string ``name`` attribute are included. If
        duplicate names are found, a ValueError is raised indicating the
        duplicate name and item type.

    Raises
    ------
    ValueError
        If duplicate names are found.
    """
    indexed: dict[str, ItemT] = {}
    for item in items:
        if not isinstance(name := getattr(item, 'name', None), str) or not name:
            continue
        if name in indexed:
            raise ValueError(f'Duplicate {item_label} name: {name}')
        indexed[name] = item
    return indexed


def merge_mapping_options(
    *option_sets: object,
    excluded_keys: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Merge mapping-like option sets with later mappings taking precedence."""
    merged: dict[str, Any] = {}
    for option_set in option_sets:
        if isinstance(option_set, Mapping):
            merged.update(option_set)
    for key in excluded_keys:
        merged.pop(key, None)
    return merged
