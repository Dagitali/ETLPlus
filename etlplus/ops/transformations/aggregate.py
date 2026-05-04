"""
:mod:`etlplus.ops.transformations.aggregate` module.

Aggregate helpers shared by :mod:`etlplus.ops.transform` and custom runners.

Use :func:`apply_aggregate` for a single aggregate spec that returns one
mapping. Use :func:`apply_aggregate_step` when you need the one-row list shape
consumed by :func:`etlplus.ops.transform.transform`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import cast

from ...utils._types import JSONDict
from ...utils._types import JSONList
from .._enums import AggregateName
from .._types import AggregateFunc
from .._types import AggregateSpec
from .._types import FieldName

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'apply_aggregate',
    'apply_aggregate_step',
]


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _agg_avg(
    nums: list[float],
    _: int,
) -> float:
    """
    Return the average of *nums* or ``0.0`` if empty.

    Parameters
    ----------
    nums : list[float]
        Numeric values to average.

    Returns
    -------
    float
        The average of the input numbers or ``0.0`` if empty.
    """
    return (sum(nums) / len(nums)) if nums else 0.0


def _agg_count(
    _: list[float],
    present: int,
) -> int:
    """
    Return the provided presence count *present*.

    Parameters
    ----------
    present : int
        Count of present values.

    Returns
    -------
    int
        The provided presence count *present*.
    """
    return present


def _agg_max(
    nums: list[float],
    _: int,
) -> float | None:
    """
    Return the maximum of *nums* or ``None`` if empty.

    Parameters
    ----------
    nums : list[float]
        Numeric values to consider.

    Returns
    -------
    float | None
        The maximum of the input numbers or ``None`` if empty.
    """
    return max(nums) if nums else None


def _agg_min(
    nums: list[float],
    _: int,
) -> float | None:
    """
    Return the minimum of *nums* or ``None`` if empty.

    Parameters
    ----------
    nums : list[float]
        Numeric values to consider.

    Returns
    -------
    float | None
        The minimum of the input numbers or ``None`` if empty.
    """
    return min(nums) if nums else None


def _agg_sum(
    nums: list[float],
    _: int,
) -> float:
    """
    Return the sum of *nums* or ``0.0`` if empty.

    Parameters
    ----------
    nums : list[float]
        Numeric values to sum.

    Returns
    -------
    float
        The sum of the input numbers or ``0.0`` if empty.
    """
    return sum(nums)


def _resolve_aggregator(
    func: AggregateName | AggregateFunc | str,
) -> AggregateFunc:
    """
    Resolve an aggregate specifier to a callable.

    Parameters
    ----------
    func : AggregateName | AggregateFunc | str
        An :class:`AggregateName`, a string (with aliases), or a callable.

    Returns
    -------
    AggregateFunc
        Function of signature ``(xs: list[float], n: int) -> Any``.

    Raises
    ------
    TypeError
        If *func* cannot be interpreted as an aggregator.
    """
    if isinstance(func, AggregateName):
        return func.func
    if isinstance(func, str):
        return AggregateName.coerce(func).func
    if callable(func):
        return cast(AggregateFunc, func)

    raise TypeError(f'Invalid aggregate func: {func!r}')


def _collect_numeric_and_presence(
    rows: JSONList,
    field: FieldName | None,
) -> tuple[list[float], int]:
    """
    Collect numeric values and count presence of field in rows.

    If field is None, returns ([], len(rows)).

    Parameters
    ----------
    rows : JSONList
        Input records.
    field : FieldName | None
        Field name to check for presence.

    Returns
    -------
    tuple[list[float], int]
        A tuple containing a list of numeric values and the count of present
        fields.
    """
    if not field:
        return [], len(rows)

    nums: list[float] = []
    present = 0
    for record in rows:
        if field in record:
            present += 1
            value = record.get(field)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                nums.append(float(value))
    return nums, present


def _derive_agg_key(
    func_raw: AggregateName | AggregateFunc | str,
    field: FieldName | None,
    alias: Any,
) -> str:
    """
    Derive the output key name for an aggregate.

    Uses alias when provided; otherwise builds like "sum_amount" or "count".

    Parameters
    ----------
    func_raw : AggregateName | AggregateFunc | str
        The raw function specifier.
    field : FieldName | None
        The field being aggregated.
    alias : Any
        Optional alias for the output key.

    Returns
    -------
    str
        The derived output key name.
    """
    if alias is not None:
        return str(alias)

    if isinstance(func_raw, AggregateName):
        label = func_raw.value
    elif isinstance(func_raw, str):
        label = AggregateName.coerce(func_raw).value
    elif callable(func_raw):
        label = getattr(func_raw, '__name__', 'custom')
    else:
        label = str(func_raw)

    return label if not field else f'{label}_{field}'


# SECTION: FUNCTIONS ======================================================== #


def apply_aggregate(
    records: JSONList,
    operation: AggregateSpec,
) -> JSONDict:
    """
    Aggregate a numeric field or count presence.

    Parameters
    ----------
    records : JSONList
        Records to aggregate.
    operation : AggregateSpec
        Dict with keys ``field`` and ``func``. ``func`` is one of
        ``'sum'``, ``'avg'``, ``'min'``, ``'max'``, or ``'count'``.
        A callable may also be supplied for ``func``. Optionally, set
        ``alias`` to control the output key name.

    Returns
    -------
    JSONDict
        A single-row result like ``{"sum_age": 42}``.

    Notes
    -----
    Numeric operations ignore non-numeric values but count their presence
    for ``'count'``.
    """
    field = operation.get('field')
    func = operation.get('func')
    alias = operation.get('alias')

    if not field or func is None:
        return {'error': 'Invalid aggregation operation'}

    try:
        aggregator = _resolve_aggregator(func)
    except TypeError:
        return {'error': f'Unknown aggregation function: {func}'}

    nums, present = _collect_numeric_and_presence(records, field)
    key_name = _derive_agg_key(func, field, alias)
    return {key_name: aggregator(nums, present)}


def apply_aggregate_step(
    rows: JSONList,
    spec: AggregateSpec,
) -> JSONList:
    """
    Apply a single aggregate pipeline step and return a one-row result list.

    Parameters
    ----------
    rows : JSONList
        Input records.
    spec : AggregateSpec
        Mapping with keys like ``{'field': 'amount', 'func': 'sum', 'alias':
        'total'}``.

    Returns
    -------
    JSONList
        A list containing one mapping ``[{alias: value}]`` so callers can reuse
        the same adapter shape as :func:`etlplus.ops.transform.transform`.
    """
    if not isinstance(spec, Mapping):
        return rows

    field: FieldName | None = spec.get('field')  # type: ignore[assignment]
    func_raw = spec.get('func', 'count')
    alias = spec.get('alias')

    agg_func = _resolve_aggregator(func_raw)
    nums, present = _collect_numeric_and_presence(rows, field)
    key = _derive_agg_key(func_raw, field, alias)
    result = agg_func(nums, present)
    return [{key: result}]
