"""
ETLPlus Data Transformation
===========================

Helpers to filter, map/rename, select, sort, aggregate, and otherwise
transform JSON-like records (dicts and lists of dicts).
"""
from __future__ import annotations

import operator
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import cast

from .load import load_data as _load_data
from .types import AggregateFunc
from .types import AggregateName
from .types import AggregateSpec
from .types import FieldName
from .types import Fields
from .types import FilterSpec
from .types import JSONData
from .types import JSONDict
from .types import JSONList
from .types import MapSpec
from .types import OperatorFunc
from .types import OperatorName
from .types import PipelineConfig
from .types import PipelineStepName
from .types import SortKey
from .types import StepApplier
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
) -> SortKey:
    """
    Coerce mixed-type values into a sortable tuple key.

    Parameters
    ----------
    value : Any
        Value to normalize for sorting.

    Returns
    -------
    SortKey
        A key that sorts numbers before strings; ``None`` sorts last.

    """
    if value is None:
        return (1, '')
    if isinstance(value, (int, float)):
        return (0, value)

    return (0, str(value))


def _normalize_specs(
    config: Any,
) -> list[Any]:
    if config is None:
        return []
    if isinstance(config, Sequence) and not isinstance(
        config, (str, bytes, bytearray),
    ):
        return list(config)
    return [config]


def _apply_filter_step(
    data: JSONList,
    spec: Any,
) -> JSONList:
    if isinstance(spec, Mapping):
        return apply_filter(data, spec)
    return data


def _apply_map_step(
    data: JSONList,
    spec: Any,
) -> JSONList:
    if isinstance(spec, Mapping):
        return apply_map(data, spec)
    return data


def _apply_select_step(
    data: JSONList,
    spec: Any,
) -> JSONList:
    fields: Sequence[Any]
    if isinstance(spec, Mapping):
        maybe_fields = spec.get('fields')
        if not isinstance(maybe_fields, Sequence) or isinstance(
            maybe_fields, (str, bytes, bytearray),
        ):
            return data
        fields = maybe_fields
    elif isinstance(spec, Sequence) and not isinstance(
        spec, (str, bytes, bytearray),
    ):
        fields = spec
    else:
        return data

    return apply_select(data, [str(field) for field in fields])


def _apply_sort_step(
    data: JSONList,
    spec: Any,
) -> JSONList:
    if isinstance(spec, Mapping):
        field_value = spec.get('field')
        field = str(field_value) if field_value is not None else None
        reverse = bool(spec.get('reverse', False))
        return apply_sort(data, field, reverse)

    if spec is None:
        return data

    return apply_sort(data, str(spec), False)


# SECTION: PROTECTED CONSTANTS ============================================== #


_AGGREGATE_FUNCS: dict[AggregateName, AggregateFunc] = {
    'sum': _agg_sum,
    'avg': _agg_avg,
    'min': _agg_min,
    'max': _agg_max,
    'count': _agg_count,
}


_OPERATORS: dict[OperatorName, OperatorFunc] = {
    'eq': operator.eq,
    'ne': operator.ne,
    'gt': operator.gt,
    'gte': operator.ge,
    'lt': operator.lt,
    'lte': operator.le,
    'in': _has,
    'contains': _contains,
}


_PIPELINE_STEPS: tuple[PipelineStepName, ...] = (
    'filter',
    'map',
    'select',
    'sort',
    'aggregate',
)


_STEP_APPLIERS: dict[PipelineStepName, StepApplier] = {
    'filter': _apply_filter_step,
    'map': _apply_map_step,
    'select': _apply_select_step,
    'sort': _apply_sort_step,
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
    condition: FilterSpec,
) -> JSONList:
    """
    Filter a list of records by a simple condition.

    Parameters
    ----------
    data : JSONList
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

    field_name = condition.get('field')
    op_raw = condition.get('op')
    value = condition.get('value')

    if not field_name or op_raw is None or value is None:
        return data

    op_func: OperatorFunc | None
    if callable(op_raw):
        op_func = cast(OperatorFunc, op_raw)
    else:
        # Normalize and look up by declared OperatorName set
        name = str(op_raw).lower()
        op_func = _OPERATORS.get(cast(OperatorName, name))

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
    mapping: MapSpec,
) -> JSONList:
    """
    Map/rename fields in each record.

    Parameters
    ----------
    data : JSONList
        Records to transform.
    mapping : MapSpec
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
    fields: Fields,
) -> JSONList:
    """
    Keep only the requested fields in each record.

    Parameters
    ----------
    data : JSONList
        Records to project.
    fields : Fields
        Field names to retain.

    Returns
    -------
    JSONList
        Records containing the requested fields; missing fields are ``None``.
    """

    return [{field: item.get(field) for field in fields} for item in data]


def apply_sort(
    data: JSONList,
    field: FieldName | None,
    reverse: bool = False,
) -> JSONList:
    """
    Sort records by a field.

    Parameters
    ----------
    data : JSONList
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
        return data

    key_field: FieldName = field
    return sorted(
        data,
        key=lambda x: _sort_key(x.get(key_field)),
        reverse=reverse,
    )


def apply_aggregate(
    data: JSONList,
    operation: AggregateSpec,
) -> JSONDict:
    """
    Aggregate a numeric field or count presence.

    Parameters
    ----------
    data : JSONList
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

    aggregator: AggregateFunc | None
    func_label: str

    if callable(func):
        aggregator = cast(AggregateFunc, func)
        func_label = getattr(func, '__name__', 'custom')
    else:
        func_label = str(func).lower()
        aggregator = _AGGREGATE_FUNCS.get(cast(AggregateName, func_label))

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
    key_name = (
        str(alias) if alias is not None else f"{func_label}_{field_name}"
    )

    return {key_name: aggregator(nums, present)}


def transform(
    source: StrPath | JSONData,
    operations: PipelineConfig | None = None,
) -> JSONData:
    """
    Transform data using optional filter/map/select/sort/aggregate steps.

    Parameters
    ----------
    source : StrPath | JSONData
        Data source to transform.
    operations : PipelineConfig or None, optional
        Operation dictionary that may contain the keys ``filter``, ``map``,
        ``select``, ``sort``, and ``aggregate`` with their respective
        configs. Each value may be a single config or a sequence of configs
        to apply in order. Aggregations accept multiple configs and merge
        the results.

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
        for step in _PIPELINE_STEPS:
            raw_spec = operations.get(step)
            if raw_spec is None:
                continue

            specs = _normalize_specs(raw_spec)
            if not specs:
                continue

            if step == 'aggregate':
                combined: JSONDict = {}
                for spec in specs:
                    if not isinstance(spec, Mapping):
                        continue
                    result = apply_aggregate(data, spec)
                    if 'error' in result:
                        return result
                    combined.update(result)
                if combined:
                    return combined
                continue

            applier: StepApplier | None = _STEP_APPLIERS.get(step)
            if applier is None:
                continue

            for spec in specs:
                data = applier(data, spec)

    # Convert back to single dict if input was single dict
    if is_single_dict and isinstance(data, list) and len(data) == 1:
        return data[0]

    return data
