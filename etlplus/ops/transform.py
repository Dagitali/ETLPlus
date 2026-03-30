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

    from etlplus.ops import (
        PipelineStep,
        OperatorName,
        AggregateName,
    )
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

from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import cast

from ..utils._types import JSONData
from ..utils._types import JSONDict
from ..utils._types import StrPath
from ._enums import PipelineStep
from ._types import PipelineConfig
from ._types import PipelineStepName
from ._types import StepApplier
from ._types import StepOrSteps
from ._types import StepSpec
from .load import load_data
from .transformations.aggregate import apply_aggregate
from .transformations.aggregate import apply_aggregate_step
from .transformations.filter import apply_filter
from .transformations.filter import apply_filter_step
from .transformations.map import apply_map
from .transformations.map import apply_map_step
from .transformations.select import apply_select
from .transformations.select import apply_select_step
from .transformations.select import is_plain_fields_list
from .transformations.select import is_sequence_not_text
from .transformations.sort import apply_sort
from .transformations.sort import apply_sort_step

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'apply_aggregate',
    'apply_filter',
    'apply_map',
    'apply_select',
    'apply_sort',
    'transform',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_PIPELINE_STEPS: tuple[PipelineStepName, ...] = (
    'aggregate',
    'filter',
    'map',
    'select',
    'sort',
)


_STEP_APPLIERS: dict[PipelineStepName, StepApplier] = {
    'aggregate': apply_aggregate_step,
    'filter': apply_filter_step,
    'map': apply_map_step,
    'select': apply_select_step,
    'sort': apply_sort_step,
}

# SECTION: INTERNAL FUNCTIONS ============================================== #


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
    if is_sequence_not_text(config):
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


# SECTION: FUNCTIONS ======================================================== #


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
    :class:`PipelineStep` enum members. Steps are evaluated in the fixed order
    ``aggregate``, ``filter``, ``map``, ``select``, ``sort``. When the
    aggregate step is present, it returns a **single mapping** with merged
    aggregate results and row-wise steps are not applied afterward.

    Examples
    --------
    Row-wise pipeline example::

        ops = {
            'filter': {'field': 'age', 'op': 'gt', 'value': 18},
            'map': {'old_name': 'new_name'},
            'select': ['name', 'age'],
            'sort': {'field': 'name', 'reverse': False},
        }
        result = transform(data, ops)

    Aggregate-only summary::

        ops = {
            'aggregate': {'field': 'age', 'func': 'avg', 'alias': 'avg_age'},
        }
        result = transform(data, ops)

    Using enums for keys and functions::

        from etlplus.ops import (
            PipelineStep,
            OperatorName,
        )
        ops = {
            PipelineStep.FILTER: {
                'field': 'age', 'op': OperatorName.GTE, 'value': 18
            },
            PipelineStep.SORT: {
                'field': 'age', 'reverse': True
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
                    out_rows = apply_aggregate_step(data, spec)
                    if out_rows and isinstance(out_rows[0], Mapping):
                        combined.update(cast(JSONDict, out_rows[0]))
                if combined:
                    return combined
                continue

            # Special-case: plain list/tuple of field names for 'select'.
            if step == 'select' and is_plain_fields_list(raw_spec):
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
