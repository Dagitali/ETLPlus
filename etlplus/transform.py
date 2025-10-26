"""
ETLPlus Data Transformation
===========================

Utilities to filter, map/rename, select, sort, aggregate, and otherwise
transform JSON-like records (dicts and lists of dicts).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from typing import Mapping
from typing import TypeAlias

# Type aliases for clarity
JSONDict: TypeAlias = dict[str, Any]
JSONList: TypeAlias = list[JSONDict]
Data: TypeAlias = JSONDict | JSONList


def load_data(
    source: str | Data,
) -> Data:
    """
    Load data from a file path, JSON string, or direct object.

    Parameters
    ----------
    source : str or dict[str, Any] or list[dict[str, Any]]
        Data source. If a path exists, JSON is read from the file. If a
        string that is not a path, it is parsed as JSON. Dicts or lists are
        returned as-is.

    Returns
    -------
    dict[str, Any] or list[dict[str, Any]]
        Parsed object or list of objects.

    Raises
    ------
    ValueError
        If the input cannot be interpreted as a JSON object or array.
    """
    if isinstance(source, (dict, list)):
        return source

    # Try to load from file
    try:
        path = Path(source)
        if path.exists():
            with path.open(encoding='utf-8') as f:
                loaded = json.load(f)
                if isinstance(loaded, (dict, list)):
                    return loaded
                raise ValueError(
                    'JSON root must be an object or array when '
                    'loading file',
                )
    except (OSError, json.JSONDecodeError):
        pass

    # Try to parse as JSON string
    try:
        loaded = json.loads(source)
        if isinstance(loaded, (dict, list)):
            return loaded
        raise ValueError(
            'JSON root must be an object or array when parsing string',
        )
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid data source: {source}") from exc


def apply_filter(
    data: JSONList,
    condition: Mapping[str, Any],
) -> JSONList:
    """
    Filter a list of records by a simple condition.

    Parameters
    ----------
    data : list[dict[str, Any]]
        Records to filter.
    condition : Mapping[str, Any]
        Condition object with keys ``field``, ``op``, and ``value``. The
        ``op`` can be one of ``'eq'``, ``'ne'``, ``'gt'``, ``'gte'``,
        ``'lt'``, ``'lte'``, ``'in'``, or ``'contains'``.

    Returns
    -------
    list[dict[str, Any]]
        Filtered records.
    """
    field = condition.get('field')
    op = condition.get('op')
    value = condition.get('value')

    if not (field and op and (value is not None)):
        return data

    def _contains(container: Any, member: Any) -> bool:
        try:
            return member in container  # type: ignore[operator]
        except TypeError:
            return False

    def _has(member: Any, container: Any) -> bool:
        try:
            return member in container  # type: ignore[operator]
        except TypeError:
            return False

    operators: dict[str, Any] = {
        'eq': lambda x, y: x == y,
        'ne': lambda x, y: x != y,
        'gt': lambda x, y: x > y,
        'gte': lambda x, y: x >= y,
        'lt': lambda x, y: x < y,
        'lte': lambda x, y: x <= y,
        # x must be in y (e.g., value list contains field value)
        'in': lambda x, y: _has(x, y),
        # x contains y (strings/lists only)
        'contains': lambda x, y: _contains(x, y),
    }

    op_func = operators.get(str(op))
    if not op_func:
        return data

    result: JSONList = []
    for item in data:
        if field in item and op_func(item[field], value):
            result.append(item)
    return result


def apply_map(
    data: JSONList,
    mapping: Mapping[str, str],
) -> JSONList:
    """
    Map/rename fields in each record.

    Parameters
    ----------
    data : list[dict[str, Any]]
        Records to transform.
    mapping : Mapping[str, str]
        Mapping of old field names to new field names.

    Returns
    -------
    list[dict[str, Any]]
        New records with keys renamed. Unmapped fields are preserved.
    """
    result: JSONList = []
    for item in data:
        new_item: JSONDict = {}
        for old_key, new_key in mapping.items():
            if old_key in item:
                new_item[new_key] = item[old_key]
        # Keep fields not in mapping
        for key, value in item.items():
            if key not in mapping:
                new_item[key] = value
        result.append(new_item)
    return result


def apply_select(
    data: JSONList,
    fields: list[str],
) -> JSONList:
    """
    Keep only the requested fields in each record.

    Parameters
    ----------
    data : list[dict[str, Any]]
        Records to project.
    fields : list[str]
        Field names to retain.

    Returns
    -------
    list[dict[str, Any]]
        Records containing the requested fields; missing fields are ``None``.
    """
    return [{field: item.get(field) for field in fields} for item in data]


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


def apply_sort(
    data: JSONList,
    field: str | None,
    reverse: bool = False,
) -> JSONList:
    """
    Sort records by a field.

    Parameters
    ----------
    data : list[dict[str, Any]]
        Records to sort.
    field : str or None
        Field name to sort by. If ``None``, input is returned unchanged.
    reverse : bool, optional
        Sort descending if ``True``. Default is ``False``.

    Returns
    -------
    list[dict[str, Any]]
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
    data : list[dict[str, Any]]
        Records to aggregate.
    operation : Mapping[str, Any]
        Dict with keys ``field`` and ``func``. ``func`` is one of
        ``'sum'``, ``'avg'``, ``'min'``, ``'max'``, or ``'count'``.

    Returns
    -------
    dict[str, Any]
        A single-row result like ``{"sum_age": 42}``.

    Notes
    -----
    Numeric operations ignore non-numeric values but count their presence
    for ``'count'``.
    """
    field = operation.get('field')
    func = operation.get('func')

    if not field or not func:
        return {'error': 'Invalid aggregation operation'}

    nums: list[float] = []
    present: int = 0
    for item in data:
        if field in item:
            present += 1
            v = item.get(field)
            if isinstance(v, (int, float)):
                nums.append(float(v))

    if func == 'sum':
        return {f"{func}_{field}": sum(nums)}
    if func == 'avg':
        return {f"{func}_{field}": (sum(nums) / len(nums)) if nums else 0.0}
    if func == 'min':
        return {f"{func}_{field}": min(nums) if nums else None}
    if func == 'max':
        return {f"{func}_{field}": max(nums) if nums else None}
    if func == 'count':
        return {f"{func}_{field}": present}

    return {'error': f"Unknown aggregation function: {func}"}


def transform(
    source: str | Data,
    operations: Mapping[str, Any] | None = None,
) -> Data:
    """
    Transform data using optional filter/map/select/sort/aggregate steps.

    Parameters
    ----------
    source : str or dict[str, Any] or list[dict[str, Any]]
        Data source to transform.
    operations : Mapping[str, Any] or None, optional
        Operation dictionary that may contain the keys ``filter``, ``map``,
        ``select``, ``sort``, and ``aggregate`` with their respective
        configs.

    Returns
    -------
    dict[str, Any] or list[dict[str, Any]]
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
