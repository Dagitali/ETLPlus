"""
ETLPlus Data Transformation
===========================

Helpers to filter, map/rename, select, sort, aggregate, and otherwise
transform JSON-like records (dicts and lists of dicts).
"""
from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import Callable
from typing import cast

from .enums import AggregateName
from .enums import OperatorName
from .enums import PipelineStep
from .load import load_data as _load_data
from .types import AggregateFunc
from .types import AggregateSpec
from .types import Aggregator
from .types import FieldName
from .types import Fields
from .types import FilterSpec
from .types import JSONData
from .types import JSONDict
from .types import JSONList
from .types import MapSpec
from .types import Operator
from .types import OperatorFunc
from .types import PipelineConfig
from .types import PipelineStepName
from .types import SortKey
from .types import StepApplier
from .types import StepOrSteps
from .types import StepSpec
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
    config: StepOrSteps | None,
) -> list[StepSpec]:
    if config is None:
        return []
    if isinstance(config, Sequence) and not isinstance(
        config, (str, bytes, bytearray),
    ):
        # Already a sequence of step specs; normalize to a list.
        return list(config)  # type: ignore[list-item]

    # Single spec
    return [config]


# New helper to normalize operation keys to plain strings.
def _normalize_operation_keys(ops: Mapping[Any, Any]) -> dict[str, Any]:
    """Normalize pipeline operation keys to plain strings.

    Accepts both string keys (e.g., 'filter') and enum keys
    (PipelineStep.FILTER), returning a str->spec mapping.
    """
    normalized: dict[str, Any] = {}
    for k, v in ops.items():
        if isinstance(k, str):
            normalized[k] = v
        elif isinstance(k, PipelineStep):
            normalized[k.value] = v
        else:
            # Fallback: try `.value`, else use string form
            name = getattr(k, 'value', str(k))
            if isinstance(name, str):
                normalized[name] = v
    return normalized


def _apply_aggregate_step(
    rows: JSONList, spec: AggregateSpec,
) -> JSONList:
    """
    Expects spec like: {'field': 'amount', 'func': 'sum', 'alias': 'total'}
    Returns a single-row list with the aggregate result keyed by alias.
    """
    field: FieldName | None = spec.get('field')  # type: ignore[assignment]
    func_raw = spec.get('func', 'count')
    alias = spec.get('alias')

    agg_func = _resolve_aggregator(func_raw)

    # Gather numeric values if a field is provided; COUNT may ignore xs.
    if field:
        xs = []
        for r in rows:
            v = r.get(field)
            if isinstance(v, (int, float)):
                xs.append(float(v))
        n = len(rows)
    else:
        # Aggregates like COUNT still work without a field.
        xs = []
        n = len(rows)

    result = agg_func(xs, n)

    # Default alias if not provided, e.g., "sum_amount" or "count"
    if not alias:
        alias = (
            f"{AggregateName.coerce(func_raw).value}"
            + (f"_{field}" if field else '')
        )

    return [{alias: result}]


def _apply_filter_step(
    rows: JSONList,
    spec: Any,
) -> JSONList:
    """
    Expects spec like: {'field': 'price', 'op': 'gte', 'value': 10}.
    """

    field: FieldName = spec.get('field')  # type: ignore[assignment]
    op_raw = spec.get('op')
    value = spec.get('value')

    if not field:
        return rows  # or raise, depending on your policy

    op_func = _resolve_operator(op_raw)

    def _pred(r: JSONDict) -> bool:
        try:
            lhs = r[field]
        except KeyError:
            return False
        try:
            return op_func(lhs, value)
        except Exception:
            # Silent fail keeps behavior lenient; switch to raise if desired.
            return False

    return [r for r in rows if _pred(r)]

    # if isinstance(spec, Mapping):
    #     return apply_filter(data, spec)
    # return data


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
        if not _is_plain_fields_list(maybe_fields):
            return data
        fields = cast(Sequence[Any], maybe_fields)
    elif _is_plain_fields_list(spec):
        fields = cast(Sequence[Any], spec)
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


def _is_plain_fields_list(obj: Any) -> bool:
    """Return True if obj is a non-text sequence of non-mapping items.

    Used to detect a list/tuple of field names like ['name', 'age'].
    """
    return isinstance(obj, Sequence) \
        and not isinstance(obj, (str, bytes, bytearray)) \
        and not any(isinstance(x, Mapping) for x in obj)


def _resolve_aggregator(
    func: Aggregator | str,
) -> Callable:
    """
    Accepts an Aggregate enum/callable/string and returns a callable
    (xs, n)->Any.
    """
    if isinstance(func, AggregateName):
        return func.func
    if isinstance(func, str):
        return AggregateName.coerce(func).func
    if callable(func):
        return func

    raise TypeError(f'Invalid aggregate func: {func!r}')


def _resolve_operator(
    op: Operator | str,
) -> Callable:
    """
    Accepts an Operator enum/callable/string and returns a callable
    (a,b)->bool.
    """
    if isinstance(op, OperatorName):
        return op.func
    if isinstance(op, str):
        return OperatorName.coerce(op).func
    if callable(op):
        return op

    raise TypeError(f'Invalid operator: {op!r}')


# SECTION: PROTECTED CONSTANTS ============================================== #


# Thin, enum-derived compatibility maps retained for backward compatibility
# with external code that may import these names. Prefer using
# `_resolve_operator` and `_resolve_aggregator` directly.
_AGGREGATE_FUNCS: dict[str, AggregateFunc] = {
    m.value: m.func  # type: ignore[dict-item]
    for m in AggregateName
}

_OPERATORS: dict[str, OperatorFunc] = {
    m.value: m.func  # type: ignore[dict-item]
    for m in OperatorName
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

    try:
        op_func = cast(OperatorFunc, _resolve_operator(op_raw))
    except TypeError:
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

    func_label: str
    try:
        aggregator = _resolve_aggregator(func)
    except TypeError:
        return {'error': f"Unknown aggregation function: {func}"}

    if isinstance(func, AggregateName):
        func_label = func.value
    elif isinstance(func, str):
        func_label = AggregateName.coerce(func).value
    elif callable(func):
        func_label = getattr(func, '__name__', 'custom')
    else:
        func_label = str(func)

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

    ops = _normalize_operation_keys(operations)

    # Convert single dict to list for uniform processing.
    is_single_dict = isinstance(data, dict)
    if is_single_dict:
        data = [data]  # type: ignore[list-item]

    # All record-wise ops require a list of dicts.
    if isinstance(data, list):
        for step in _PIPELINE_STEPS:
            raw_spec = ops.get(step)
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
                    # Use enum-based applier that returns a single-row list
                    # like: [{alias: value}]
                    out_rows = _apply_aggregate_step(data, spec)
                    if out_rows and isinstance(out_rows[0], Mapping):
                        combined.update(cast(JSONDict, out_rows[0]))
                if combined:
                    return combined
                continue

            # Special-case: plain list/tuple of field names for 'select'.
            if step == 'select' and _is_plain_fields_list(raw_spec):
                # Keep the whole fields list as a single spec.
                specs = [cast(StepSpec, raw_spec)]

            applier: StepApplier | None = _STEP_APPLIERS.get(step)
            if applier is None:
                continue

            for spec in specs:
                data = applier(data, spec)

    # Convert back to single dict if input was single dict.
    if is_single_dict and isinstance(data, list) and len(data) == 1:
        return data[0]

    return data
