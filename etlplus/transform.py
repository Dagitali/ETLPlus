"""
ETLPlus Data Transformation
===========================

Helpers to filter, map/rename, select, sort, aggregate, and otherwise
transform JSON-like records (dicts and lists of dicts).
"""
from __future__ import annotations

import operator
from typing import Any
from typing import Mapping

from .load import load_data as _load_data
from .types import AggregateFunc
from .types import JSONData
from .types import JSONDict
from .types import JSONList
from .types import OperatorFunc
from .types import StrPath


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _contains(
    container: Any,
    member: Any,
) -> bool:
    try:
        return member in container  # type: ignore[operator]
    except TypeError:
        return False


def _has(
    member: Any,
    container: Any,
) -> bool:
    try:
        return member in container  # type: ignore[operator]
    except TypeError:
        return False


def _agg_avg(
    nums: list[float],
    _: int,
) -> float:
    return (sum(nums) / len(nums)) if nums else 0.0


def _agg_count(
    _: list[float],
    present: int,
) -> int:
    return present


def _agg_max(
    nums: list[float],
    _: int,
) -> float | None:
    return max(nums) if nums else None


def _agg_min(
    nums: list[float],
    _: int,
) -> float | None:
    return min(nums) if nums else None


def _agg_sum(
    nums: list[float],
    _: int,
) -> float:
    return sum(nums)


def _sort_key(
    value: Any,
) -> tuple[int, Any]:
    """
    Coerce mixed-type values into a sortable tuple key.

    Parameters
    ----------
    value : Any
        Value to normalize for sorting.

    Returns
    -------
    tuple[int, Any]
        A key that sorts numbers before strings; ``None`` sorts last.

    """
    if value is None:
        return (1, '')
    if isinstance(value, (int, float)):
        return (0, value)

    return (0, str(value))


# SECTION: PROTECTED CONSTANTS ============================================== #


_OPERATORS: dict[str, OperatorFunc] = {
    'eq': operator.eq,
    'ne': operator.ne,
    'gt': operator.gt,
    'gte': operator.ge,
    'lt': operator.lt,
    'lte': operator.le,
    'in': _has,
    'contains': _contains,
}


_AGGREGATE_FUNCS: dict[str, AggregateFunc] = {
    'sum': _agg_sum,
    'avg': _agg_avg,
    'min': _agg_min,
    'max': _agg_max,
    'count': _agg_count,
}


# SECTION: FUNCTIONS ======================================================== #


def load_data(
    source: StrPath | JSONData,
) -> JSONData:
    """
    Load data from a file path, JSON string, or direct object.

    Parameters
    ----------
    source : StrPath | JSONData
        Data source. If a path exists, JSON is read from the file. If a
        string that is not a path, it is parsed as JSON. Dicts or lists are
        returned as-is.

    Returns
    -------
    JSONData
        Parsed object or list of objects.

    Raises
    ------
    ValueError
        If the input cannot be interpreted as a JSON object or array.
    """

    return _load_data(source)


def apply_filter(
    data: JSONList,
    condition: Mapping[str, Any],
) -> JSONList:
    """
    Filter a list of records by a simple condition.

    Parameters
    ----------
    data : JSONList
        Records to filter.
    condition : Mapping[str, Any]
        Condition object with keys ``field``, ``op``, and ``value``. The
        ``op`` can be one of ``'eq'``, ``'ne'``, ``'gt'``, ``'gte'``,
        ``'lt'``, ``'lte'``, ``'in'``, or ``'contains'``.

    Returns
    -------
    JSONList
        Filtered records.
    """

    field_name = condition.get('field')
    op_name = condition.get('op')
    value = condition.get('value')

    if not field_name or op_name is None or value is None:
        return data

    op_func = _OPERATORS.get(str(op_name).lower())
    if not op_func:
        return data

    result: JSONList = []
    for item in data:
        if field_name not in item:
            continue
        try:
            if op_func(item[field_name], value):
                result.append(item)
        except TypeError:
            # Skip records where the comparison is not supported.
            continue

    return result


def apply_map(
    data: JSONList,
    mapping: Mapping[str, str],
) -> JSONList:
    """
    Map/rename fields in each record.

    Parameters
    ----------
    data : JSONList
        Records to transform.
    mapping : Mapping[str, str]
        Mapping of old field names to new field names.

    Returns
    -------
    JSONList
        New records with keys renamed. Unmapped fields are preserved.
    """

    rename_map = dict(mapping)
    result: JSONList = []

    for item in data:
        renamed = {
            new_key: item[old_key]
            for old_key, new_key in rename_map.items()
            if old_key in item
        }
        renamed.update({
            key: value
            for key, value in item.items()
            if key not in rename_map
        })
        result.append(renamed)

    return result


def apply_select(
    data: JSONList,
    fields: list[str],
) -> JSONList:
    """
    Keep only the requested fields in each record.

    Parameters
    ----------
    data : JSONList
        Records to project.
    fields : list[str]
        Field names to retain.

    Returns
    -------
    JSONList
        Records containing the requested fields; missing fields are ``None``.
    """

    return [{field: item.get(field) for field in fields} for item in data]


def apply_sort(
    data: JSONList,
    field: str | None,
    reverse: bool = False,
) -> JSONList:
    """
    Sort records by a field.

    Parameters
    ----------
    data : JSONList
        Records to sort.
    field : str | None
        Field name to sort by. If ``None``, input is returned unchanged.
    reverse : bool, optional
        Sort descending if ``True``. Default is ``False``.

    Returns
    -------
    JSONList
        Sorted records.
    """

    if not field:
        return data

    key_field: str = field
    return sorted(
        data,
        key=lambda x: _sort_key(x.get(key_field)),
        reverse=reverse,
    )


def apply_aggregate(
    data: JSONList,
    operation: Mapping[str, Any],
) -> JSONDict:
    """
    Aggregate a numeric field or count presence.

    Parameters
    ----------
    data : JSONList
        Records to aggregate.
    operation : Mapping[str, Any]
        Dict with keys ``field`` and ``func``. ``func`` is one of
        ``'sum'``, ``'avg'``, ``'min'``, ``'max'``, or ``'count'``.

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

    if not field or func is None:
        return {'error': 'Invalid aggregation operation'}

    func_key = str(func).lower()
    aggregator = _AGGREGATE_FUNCS.get(func_key)
    if aggregator is None:
        return {'error': f"Unknown aggregation function: {func}"}

    nums: list[float] = []
    present = 0
    for item in data:
        if field in item:
            present += 1
            v = item.get(field)
            if isinstance(v, (int, float)):
                nums.append(float(v))

    field_name = str(field)
    return {f"{func_key}_{field_name}": aggregator(nums, present)}


def transform(
    source: StrPath | JSONData,
    operations: Mapping[str, Any] | None = None,
) -> JSONData:
    """
    Transform data using optional filter/map/select/sort/aggregate steps.

    Parameters
    ----------
    source : StrPath | JSONData
        Data source to transform.
    operations : Mapping[str, Any] or None, optional
        Operation dictionary that may contain the keys ``filter``, ``map``,
        ``select``, ``sort``, and ``aggregate`` with their respective
        configs.

    Returns
    -------
    JSONData
        Transformed data.

    Examples
    --------
    Minimal example with multiple steps::

        ops = {
            'filter': {'field': 'age', 'op': 'gt', 'value': 18},
            'map': {'old_name': 'new_name'},
            'select': ['name', 'age'],
            'sort': {'field': 'name', 'reverse': False},
            'aggregate': {'field': 'age', 'func': 'avg'},
        }
        result = transform(data, ops)
    """

    data = load_data(source)

    if not operations:
        return data

    # Convert single dict to list for uniform processing
    is_single_dict = isinstance(data, dict)
    if is_single_dict:
        data = [data]  # type: ignore[list-item]

    # All record-wise ops require a list of dicts
    if isinstance(data, list):
        if 'filter' in operations:
            data = apply_filter(
                data, operations['filter'],  # type: ignore[arg-type]
            )

        if 'map' in operations:
            data = apply_map(
                data, operations['map'],  # type: ignore[arg-type]
            )

        if 'select' in operations:
            data = apply_select(
                data, operations['select'],  # type: ignore[arg-type]
            )

        if 'sort' in operations:
            sort_cfg = operations['sort']
            if isinstance(sort_cfg, Mapping):
                data = apply_sort(
                    data,
                    str(sort_cfg.get('field'))
                    if sort_cfg.get('field') is not None
                    else None,
                    bool(sort_cfg.get('reverse', False)),
                )
            else:
                # allow shorthand: "name" -> sort by field name
                data = apply_sort(data, str(sort_cfg))

        if 'aggregate' in operations:
            return apply_aggregate(
                data, operations['aggregate'],  # type: ignore[arg-type]
            )

    # Convert back to single dict if input was single dict
    if is_single_dict and isinstance(data, list) and len(data) == 1:
        return data[0]

    return data
