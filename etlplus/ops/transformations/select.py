"""
:mod:`etlplus.ops.transformations.select` module.

Select transformation helpers extracted from :mod:`etlplus.ops.transform`.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import cast

from ...utils._types import JSONList
from .._types import Fields

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'apply_select',
    'apply_select_step',
    'is_plain_fields_list',
    'is_sequence_not_text',
]


# SECTION: FUNCTIONS ======================================================== #


def is_sequence_not_text(
    obj: Any,
) -> bool:
    """
    Return ``True`` for non-text sequences.

    Parameters
    ----------
    obj : Any
        The object to check.

    Returns
    -------
    bool
        ``True`` when *obj* is a non-text sequence.
    """
    return isinstance(obj, Sequence) and not isinstance(
        obj,
        (str, bytes, bytearray),
    )


def is_plain_fields_list(
    obj: Any,
) -> bool:
    """
    Return True if *obj* is a non-text sequence of non-mapping items.

    Used to detect a list/tuple of field names like ['name', 'age'].

    Parameters
    ----------
    obj : Any
        The object to check.

    Returns
    -------
    bool
        True if obj is a non-text sequence of non-mapping items, False
        otherwise.
    """
    return is_sequence_not_text(obj) and not any(isinstance(x, Mapping) for x in obj)


def apply_select(
    records: JSONList,
    fields: Fields,
) -> JSONList:
    """
    Keep only the requested fields in each record.

    Parameters
    ----------
    records : JSONList
        Records to project.
    fields : Fields
        Field names to retain.

    Returns
    -------
    JSONList
        Records containing the requested fields; missing fields are ``None``.
    """
    return [{field: record.get(field) for field in fields} for record in records]


def apply_select_step(
    records: JSONList,
    spec: Any,
) -> JSONList:
    """
    Apply a functional select/project step to a list of records.

    Parameters
    ----------
    records : JSONList
        Input records to transform.
    spec : Any
        Either a mapping with key ``'fields'`` whose value is a sequence of
        field names, or a plain sequence of field names.

    Returns
    -------
    JSONList
        Transformed data.
    """
    fields: Sequence[Any]
    if isinstance(spec, Mapping):
        maybe_fields = spec.get('fields')
        if not is_plain_fields_list(maybe_fields):
            return records
        fields = cast(Sequence[Any], maybe_fields)
    elif is_plain_fields_list(spec):
        fields = cast(Sequence[Any], spec)
    else:
        return records

    return apply_select(records, [str(field) for field in fields])
