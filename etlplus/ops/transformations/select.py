"""
:mod:`etlplus.ops.transformations.select` module.

Select helpers shared by :mod:`etlplus.ops.transform` and custom runners.

Use :func:`apply_select` for direct field projection. Use
:func:`apply_select_step` when you need the pipeline-style adapter consumed by
:func:`etlplus.ops.transform.transform`. The normalization helper
:func:`is_plain_fields_list` and compatibility alias
:func:`is_sequence_not_text` are public for callers that need to validate
select-step configs before orchestration.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import cast

from ...utils import SequenceParser
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
    Apply a select/project pipeline step to a list of records.

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
        Projected records using the same step semantics as
        :func:`etlplus.ops.transform.transform`.
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


def is_plain_fields_list(
    obj: Any,
) -> bool:
    """
    Return ``True`` if *obj* is a non-text sequence of non-mapping items.

    Used to detect a list or tuple of field names such as ``['name', 'age']``
    when normalizing select specs for :func:`etlplus.ops.transform.transform`.

    Parameters
    ----------
    obj : Any
        The object to check.

    Returns
    -------
    bool
        ``True`` if *obj* is a non-text sequence of non-mapping items;
        ``False`` otherwise.
    """
    return SequenceParser.is_non_text(obj) and not any(
        isinstance(x, Mapping) for x in obj
    )


def is_sequence_not_text(
    obj: object,
) -> bool:
    """
    Return ``True`` for non-text sequences.

    Used as a compatibility alias for callers that want to check for a sequence
    excluding text and byte strings without importing :class:`SequenceParser`.

    Parameters
    ----------
    obj : object
        Input value to test.

    Returns
    -------
    bool
        ``True`` if *obj* is a non-text sequence; ``False`` otherwise.
    """
    return SequenceParser.is_non_text(obj)
