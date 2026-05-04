"""
:mod:`etlplus.ops.transformations.filter` module.

Filter helpers shared by :mod:`etlplus.ops.transform` and custom runners.

Use :func:`apply_filter` for direct record filtering. Use
:func:`apply_filter_step` when you need the pipeline-style adapter consumed by
:func:`etlplus.ops.transform.transform`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import cast

from ...utils import FloatParser
from ...utils._types import JSONDict
from ...utils._types import JSONList
from .._enums import OperatorName
from .._types import FieldName
from .._types import FilterSpec
from .._types import OperatorFunc

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'apply_filter',
    'apply_filter_step',
]


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _contains(
    container: Any,
    member: Any,
) -> bool:
    """
    Return ``True`` if *member* is contained in *container*.

    Parameters
    ----------
    container : Any
        Potential container object.
    member : Any
        Candidate member to check for containment.

    Returns
    -------
    bool
        ``True`` if ``member in container`` succeeds; ``False`` on
        ``TypeError`` or when containment fails.
    """
    try:
        return member in container  # type: ignore[operator]
    except TypeError:
        return False


def _eval_condition(
    record: JSONDict,
    field: FieldName,
    op_func: OperatorFunc,
    value: Any,
    catch_all: bool,
) -> bool:
    """
    Evaluate a filter condition on a record.

    Returns False if the field is missing or if the operator raises.

    Parameters
    ----------
    record : JSONDict
        The input record.
    field : FieldName
        The field name to check.
    op_func : OperatorFunc
        The binary operator function.
    value : Any
        The value to compare against.
    catch_all : bool
        If True, catch all exceptions and return False; if False, propagate
        exceptions.

    Returns
    -------
    bool
        True if the condition is met; False if not.

    Raises
    ------
    Exception
        If *catch_all* is False and the operator raises.
    """
    try:
        lhs = record[field]
    except KeyError:
        return False

    try:
        return bool(op_func(lhs, value))
    except Exception:  # noqa: BLE001 - controlled by flag
        if catch_all:
            return False
        raise


def _filter_records(
    records: JSONList,
    *,
    field: FieldName,
    op_func: OperatorFunc,
    value: Any,
    catch_all: bool,
) -> JSONList:
    """Return records matching one resolved predicate."""
    result: JSONList = []
    for record in records:
        if field not in record:
            continue
        try:
            if _eval_condition(record, field, op_func, value, catch_all=catch_all):
                result.append(record)
        except TypeError:
            continue
    return result


def _has(
    member: Any,
    container: Any,
) -> bool:
    """
    Return ``True`` if *container* contains *member*.

    This is the dual form of :func:`_contains` for readability in certain
    operator contexts (``in`` vs. ``contains``).
    """
    return _contains(container, member)


def _resolve_operator(
    op: OperatorName | OperatorFunc | str,
) -> OperatorFunc:
    """
    Resolve an operator specifier to a binary predicate.

    Parameters
    ----------
    op : OperatorName | OperatorFunc | str
        An :class:`OperatorName`, a string (with aliases), or a callable.

    Returns
    -------
    Callable
        Function of signature ``(a: Any, b: Any) -> bool``.

    Raises
    ------
    TypeError
        If *op* cannot be interpreted as an operator.
    """

    def _wrap_numeric(op_name: OperatorName) -> OperatorFunc:
        base = op_name.func
        if op_name in {
            OperatorName.GT,
            OperatorName.GTE,
            OperatorName.LT,
            OperatorName.LTE,
            OperatorName.EQ,
            OperatorName.NE,
        }:

            def compare(a: Any, b: Any) -> bool:  # noqa: ANN401 - generic
                a_num = FloatParser.coerce(a)
                b_num = FloatParser.coerce(b)
                if a_num is not None and b_num is not None:
                    return bool(base(a_num, b_num))
                return bool(base(a, b))

            return compare
        return base

    if isinstance(op, OperatorName):
        return _wrap_numeric(op)
    if isinstance(op, str):
        return _wrap_numeric(OperatorName.coerce(op))
    if callable(op):
        return cast(OperatorFunc, op)

    raise TypeError(f'Invalid operator: {op!r}')


# SECTION: FUNCTIONS ======================================================== #


def apply_filter(
    records: JSONList,
    condition: FilterSpec,
) -> JSONList:
    """
    Filter a list of records by a simple condition.

    Parameters
    ----------
    records : JSONList
        Records to filter.
    condition : FilterSpec
        Condition object with keys ``field``, ``op``, and ``value``. The
        ``op`` can be one of ``'eq'``, ``'ne'``, ``'gt'``, ``'gte'``,
        ``'lt'``, ``'lte'``, ``'in'``, or ``'contains'``. Custom comparison
        logic can be provided by supplying a callable for ``op``.

    Returns
    -------
    JSONList
        Filtered records.
    """
    field = condition.get('field')
    op_raw = condition.get('op')
    value = condition.get('value')

    if not field or op_raw is None or value is None:
        return records

    try:
        op_func = _resolve_operator(op_raw)
    except TypeError:
        return records

    return _filter_records(
        records,
        field=cast(FieldName, field),
        op_func=op_func,
        value=value,
        catch_all=False,
    )


def apply_filter_step(
    records: JSONList,
    spec: Any,
) -> JSONList:
    """
    Apply a filter pipeline step to a list of records.

    Parameters
    ----------
    records : JSONList
        Input records to filter.
    spec : Any
        Mapping with keys ``field``, ``op``, and ``value``. ``op`` may be a
        string, :class:`OperatorName`, or a callable.

    Returns
    -------
    JSONList
        Filtered records using the same step semantics as
        :func:`etlplus.ops.transform.transform`.
    """
    if not isinstance(spec, Mapping):
        return records

    field: FieldName = spec.get('field')  # type: ignore[assignment]
    op = spec.get('op')
    value = spec.get('value')

    if not field or op is None:
        return records

    try:
        op_func = _resolve_operator(op)
    except TypeError:
        return records

    return _filter_records(
        records,
        field=field,
        op_func=op_func,
        value=value,
        catch_all=True,
    )
