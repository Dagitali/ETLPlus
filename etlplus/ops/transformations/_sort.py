"""
:mod:`etlplus.ops.transformations._sort` module.

Sort transformation helpers extracted from :mod:`etlplus.ops.transform`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ...utils._types import JSONList
from .._types import FieldName
from .._types import SortKey

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'apply_sort',
]


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _sort_key(
    value: Any,
) -> SortKey:
    """
    Coerce mixed-type values into a sortable tuple key.

    Ordering policy
    ---------------
    1) Numbers
    2) Non-numeric values (stringified)
    3) ``None`` (last)

    Parameters
    ----------
    value : Any
        Value to normalize for sorting.

    Returns
    -------
    SortKey
        A key with a type tag to avoid cross-type comparisons.
    """
    if value is None:
        return (2, '')
    if isinstance(value, (int, float)):
        return (0, float(value))

    return (1, str(value))


def _apply_sort_step(
    records: JSONList,
    spec: Any,
) -> JSONList:
    """
    Apply a functional sort step to a list of records.

    Parameters
    ----------
    records : JSONList
        Input records to sort.
    spec : Any
        Either a mapping with keys ``'field'`` and optional ``'reverse'``, or
        a plain field name.

    Returns
    -------
    JSONList
        Sorted records.
    """
    if isinstance(spec, Mapping):
        field_value = spec.get('field')
        field = str(field_value) if field_value is not None else None
        reverse = bool(spec.get('reverse', False))
        return apply_sort(records, field, reverse)

    if spec is None:
        return records

    return apply_sort(records, str(spec), False)


# SECTION: FUNCTIONS ======================================================== #


def apply_sort(
    records: JSONList,
    field: FieldName | None,
    reverse: bool = False,
) -> JSONList:
    """
    Sort records by a field.

    Parameters
    ----------
    records : JSONList
        Records to sort.
    field : FieldName | None
        Field name to sort by. If ``None``, input is returned unchanged.
    reverse : bool, optional
        Sort descending if ``True``. Default is ``False``.

    Returns
    -------
    JSONList
        Sorted records.
    """
    if not field:
        return records

    key_field: FieldName = field
    return sorted(
        records,
        key=lambda item: _sort_key(item.get(key_field)),
        reverse=reverse,
    )
