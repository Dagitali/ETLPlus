"""
:mod:`etlplus.ops._mappings` module.

Helpers for indexing named objects and merging mapping-style options.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'merge_mapping_options',
]


# SECTION: FUNCTIONS ======================================================== #


def merge_mapping_options(
    *option_sets: object,
    excluded_keys: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """
    Merge mapping-like option sets with later mappings taking precedence.

    Parameters
    ----------
    *option_sets : object
        Any number of option sets to merge. Only those that are instances of
        :class:`Mapping` will be merged; others are ignored.
    excluded_keys : frozenset[str], optional
        A set of keys to exclude from the merged result. Defaults to an empty
        set.

    Returns
    -------
    dict[str, Any]
        A dictionary containing the merged key-value pairs from the provided
        option sets, excluding any keys specified in *excluded_keys*. If the
        same key appears in multiple option sets, the value from the last
        option set containing that key will be used.
    """
    merged: dict[str, Any] = {}
    for option_set in option_sets:
        if isinstance(option_set, Mapping):
            merged.update(option_set)
    for key in excluded_keys:
        merged.pop(key, None)
    return merged
