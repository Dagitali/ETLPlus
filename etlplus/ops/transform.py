"""
:mod:`etlplus.ops.transform` module.

Helpers to filter, map/rename, select, sort, aggregate, and otherwise
transform JSON-like records (dicts and lists of dicts).

The pipeline accepts both **string** names (e.g., ``"filter"``) and the
enum ``PipelineStep`` for operation keys. For operators and aggregates,
specs may provide **strings** (with aliases), the corresponding **enums**
``OperatorName`` / ``AggregateName``, or **callables**.

Examples
--------
Basic pipeline with strings::

    ops = {
        'filter': {'field': 'age', 'op': 'gte', 'value': 18},
        'map': {'first_name': 'name'},
        'select': ['name', 'age'],
        'sort': {'field': 'name'},
        'aggregate': {'field': 'age', 'func': 'avg', 'alias': 'avg_age'},
    }
    result = transform(data, ops)

Using enums for keys and functions::

    from etlplus.enums import PipelineStep, OperatorName, AggregateName
    ops = {
        PipelineStep.FILTER: {
            'field': 'age', 'op': OperatorName.GTE, 'value': 18
        },
        PipelineStep.AGGREGATE: {
            'field': 'age', 'func': AggregateName.AVG
        },
    }
    result = transform(data, ops)
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import cast

from ..enums import AggregateName
from ..enums import OperatorName
from ..enums import PipelineStep
from ..types import AggregateFunc
from ..types import AggregateSpec
from ..types import FieldName
from ..types import Fields
from ..types import FilterSpec
from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import MapSpec
from ..types import OperatorFunc
from ..types import PipelineConfig
from ..types import PipelineStepName
from ..types import SortKey
from ..types import StepApplier
from ..types import StepOrSteps
from ..types import StepSpec
from ..types import StrPath
from ..utils import to_number
from .load import load_data

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'apply_aggregate',
    'apply_filter',
    'apply_map',
    'apply_select',
    'apply_sort',
    'transform',
]

# SECTION: INTERNAL FUNCTIONS ============================================== #


# -- Aggregators -- #


def _agg_avg(
    nums: list[float],
    _: int,
) -> float:
    """
    Average of *nums* or ``0.0`` if empty.

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
    Maximum of *nums* or ``None`` if empty.

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
    Minimum of *nums* or ``None`` if empty.

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
    Sum of *nums* (``0.0`` for empty).

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


# -- Normalization -- #


def _normalize_specs(
    config: StepOrSteps | None,
) -> list[StepSpec]:
    """
    Normalize a step config into a list of step specs.

    Parameters
    ----------
    config : StepOrSteps | None
        ``None``, a single mapping, or a sequence of mappings.

    Returns
    -------
    list[StepSpec]
        An empty list for ``None``, otherwise a list form of *config*.
    """
    if config is None:
        return []
    if _is_sequence_not_text(config):
        # Already a sequence of step specs; normalize to a list.
        return list(cast(Sequence[StepSpec], config))

    # Single spec
    return [cast(StepSpec, config)]


def _normalize_operation_keys(ops: Mapping[Any, Any]) -> dict[str, Any]:
    """
    Normalize pipeline operation keys to plain strings.

    Accepts both string keys (e.g., 'filter') and enum keys
    (PipelineStep.FILTER), returning a str->spec mapping.

    Parameters
    ----------
    ops : Mapping[Any, Any]
        Pipeline operations to normalize.

    Returns
    -------
    dict[str, Any]
        Dictionary whose keys are normalized step names.
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


# -- Predicates -- #


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


# -- Resolvers -- #


def _resolve_aggregator(
    func: AggregateName | AggregateFunc | str,
) -> Callable:
    """
    Resolve an aggregate specifier to a callable.

    Parameters
    ----------
    func : AggregateName | AggregateFunc | str
        An :class:`AggregateName`, a string (with aliases), or a callable.

    Returns
    -------
    Callable
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
        return func

    raise TypeError(f'Invalid aggregate func: {func!r}')


def _resolve_operator(
    op: OperatorName | OperatorFunc | str,
) -> Callable:
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

    def _wrap_numeric(op_name: OperatorName) -> Callable[[Any, Any], bool]:
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
                a_num = to_number(a)
                b_num = to_number(b)
                if a_num is not None and b_num is not None:
                    return bool(base(a_num, b_num))
                return bool(base(a, b))

            return compare
        # Non-numeric operators: use base behavior
        return base

    if isinstance(op, OperatorName):
        return _wrap_numeric(op)
    if isinstance(op, str):
        return _wrap_numeric(OperatorName.coerce(op))
    if callable(op):
        return op

    raise TypeError(f'Invalid operator: {op!r}')


# -- Sorting -- #


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


# -- Aggregation and filtering -- #


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
    for r in rows:
        if field in r:
            present += 1
            v = r.get(field)
            if isinstance(v, (int, float)):
                nums.append(float(v))
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
        If True, catch all exceptions and return; if False, propagate
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


# -- Step Appliers -- #


def _apply_aggregate_step(
    rows: JSONList,
    spec: AggregateSpec,
) -> JSONList:
    """
    Apply a single aggregate spec and return a one-row result list.

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
        A list containing one mapping ``[{alias: value}]``.
    """
    field: FieldName | None = spec.get('field')  # type: ignore[assignment]
    func_raw = spec.get('func', 'count')
    alias = spec.get('alias')

    agg_func = _resolve_aggregator(func_raw)
    xs, present = _collect_numeric_and_presence(rows, field)
    key = _derive_agg_key(func_raw, field, alias)
    result = agg_func(xs, present)
    return [{key: result}]


def _apply_filter_step(
    records: JSONList,
    spec: Any,
) -> JSONList:
    """
    Functional filter applier used by the pipeline engine.

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
        Filtered records.
    """
    field: FieldName = spec.get('field')  # type: ignore[assignment]
    op = spec.get('op')
    value = spec.get('value')

    if not field:
        return records  # Or raise, depending on your policy.

    op_func = _resolve_operator(op)

    return [
        r
        for r in records
        if _eval_condition(r, field, op_func, value, catch_all=True)
    ]


def _apply_map_step(
    records: JSONList,
    spec: Any,
) -> JSONList:
    """
    Functional map/rename applier used by the pipeline engine.

    Parameters
    ----------
    records : JSONList
        Input records to transform.
    spec : Any
        Mapping of **old field names** to **new field names**.

    Returns
    -------
    JSONList
        Transformed records.
    """
    if isinstance(spec, Mapping):
        return apply_map(records, spec)

    return records


def _apply_select_step(
    records: JSONList,
    spec: Any,
) -> JSONList:
    """
    Functional select/project applier used by the pipeline engine.

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
        if not _is_plain_fields_list(maybe_fields):
            return records
        fields = cast(Sequence[Any], maybe_fields)
    elif _is_plain_fields_list(spec):
        fields = cast(Sequence[Any], spec)
    else:
        return records

    return apply_select(records, [str(field) for field in fields])


def _apply_sort_step(
    records: JSONList,
    spec: Any,
) -> JSONList:
    """
    Functional sort applier used by the pipeline engine.

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


# -- Helpers -- #


def _is_sequence_not_text(
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


def _is_plain_fields_list(
    obj: Any,
) -> bool:
    """
    Return True if obj is a non-text sequence of non-mapping items.

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
    return _is_sequence_not_text(obj) and not any(
        isinstance(x, Mapping) for x in obj
    )


# SECTION: INTERNAL CONSTANTS ============================================== #


_PIPELINE_STEPS: tuple[PipelineStepName, ...] = (
    'aggregate',
    'filter',
    'map',
    'select',
    'sort',
)


_STEP_APPLIERS: dict[PipelineStepName, StepApplier] = {
    'aggregate': _apply_aggregate_step,
    'filter': _apply_filter_step,
    'map': _apply_map_step,
    'select': _apply_select_step,
    'sort': _apply_sort_step,
}


# SECTION: FUNCTIONS ======================================================== #


# -- Helpers -- #


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
        op_func = cast(OperatorFunc, _resolve_operator(op_raw))
    except TypeError:
        return records

    result: JSONList = []
    for record in records:
        if field not in record:
            continue
        try:
            if _eval_condition(record, field, op_func, value, catch_all=False):
                result.append(record)
        except TypeError:
            # Skip records where the comparison is not supported.
            continue

    return result


def apply_map(
    records: JSONList,
    mapping: MapSpec,
) -> JSONList:
    """
    Map/rename fields in each record.

    Parameters
    ----------
    records : JSONList
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

    for record in records:
        renamed = {
            new_key: record[old_key]
            for old_key, new_key in rename_map.items()
            if old_key in record
        }
        renamed.update(
            {
                key: value
                for key, value in record.items()
                if key not in rename_map
            },
        )
        result.append(renamed)

    return result


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
    return [
        {field: record.get(field) for field in fields} for record in records
    ]


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
        key=lambda x: _sort_key(x.get(key_field)),
        reverse=reverse,
    )


# -- Orchestration -- #


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
    operations : PipelineConfig | None, optional
        Operation dictionary that may contain the keys ``filter``, ``map``,
        ``select``, ``sort``, and ``aggregate`` with their respective
        configs. Each value may be a single config or a sequence of configs
        to apply in order. Aggregations accept multiple configs and merge
        the results.

    Returns
    -------
    JSONData
        Transformed data.

    Notes
    -----
    Operation keys may be provided as strings (e.g., ``"filter"``) or as
    :class:`PipelineStep` enum members. The aggregate step returns a **single
    mapping** with merged aggregate results when present.

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

    Using enums for keys and functions::

        from etlplus.enums import PipelineStep, OperatorName, AggregateName
        ops = {
            PipelineStep.FILTER: {
                'field': 'age', 'op': OperatorName.GTE, 'value': 18
            },
            PipelineStep.AGGREGATE: {
                'field': 'age', 'func': AggregateName.AVG
            },
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
