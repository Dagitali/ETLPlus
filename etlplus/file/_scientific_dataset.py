"""
:mod:`etlplus.file._scientific_dataset` module.

Shared helpers for scientific dataset-key normalization and selection.
"""

from __future__ import annotations

from collections.abc import Iterable

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'normalize_store_dataset_keys',
    'resolve_store_dataset_key',
]


# SECTION: FUNCTIONS ======================================================== #


def normalize_store_dataset_keys(
    keys: Iterable[str],
) -> list[str]:
    """
    Normalize store keys by removing leading separators.
    """
    return [key.lstrip('/') for key in keys]


def resolve_store_dataset_key(
    keys: list[str],
    *,
    dataset: str | None,
    default_key: str,
    format_name: str,
) -> str | None:
    """
    Resolve one selected dataset key from available store keys.
    """
    if not keys:
        return None
    if dataset is not None:
        if dataset not in keys:
            raise ValueError(f'{format_name} dataset {dataset!r} not found')
        return dataset
    if default_key in keys:
        return default_key
    if len(keys) == 1:
        return keys[0]
    raise ValueError(
        f'Multiple datasets found in {format_name} file; expected '
        f'"{default_key}" or a single dataset',
    )
