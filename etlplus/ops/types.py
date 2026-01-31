"""
:mod:`etlplus.ops.types` module.

Shared type aliases leveraged across :mod:`etlplus.ops` modules.

Notes
-----
- Centralizes ops-focused aliases (functions, specs, and pipeline helpers).
- Relies on Python 3.13 ``type`` statements for readability and IDE support.

Examples
--------
>>> from etlplus.ops.types import AggregateFunc, OperatorFunc
>>> def total(xs: list[float], _: int) -> float:
...     return sum(xs)
>>> agg: AggregateFunc = total
>>> op: OperatorFunc = lambda a, b: a == b
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import Literal

from ..types import JSONList
from ..types import StrAnyMap
from ..types import StrSeqMap
from ..types import StrStrMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Type Aliases (Functions)
    'AggregateFunc',
    'OperatorFunc',
    # Type Aliases (Records & Fields)
    'FieldName',
    'Fields',
    # Type Aliases (Transform Specs)
    'AggregateSpec',
    'FilterSpec',
    'MapSpec',
    'SelectSpec',
    'SortSpec',
    # Type Aliases (Pipelines)
    'StepOrSteps',
    'StepSeq',
    'StepSpec',
    'PipelineConfig',
    'PipelineStepName',
    # Type Aliases (Helpers)
    'StepApplier',
    'SortKey',
]


# SECTION: TYPE ALIASES ===================================================== #


# -- Functions -- #


# TODO: Consider redefining to use `functools.reduce` signature.
# TODO: Consider adding `**kwargs` to support richer aggregation functions.
# TODO: Consider constraining first argument to `Sequence[float]`.
# TODO: Consider constraining return type to `float | int | None`.
# Callable reducing numeric collections into a summary value.
type AggregateFunc = Callable[[list[float], int], Any]

# Binary predicate consumed by filter operations.
type OperatorFunc = Callable[[Any, Any], bool]

# -- Records & Fields -- #

# Individual field identifier referenced inside specs.
type FieldName = str

# Ordered list of :data:`FieldName` entries preserving projection order.
type Fields = list[FieldName]

# -- Transform Specs -- #

# Filtering spec expecting ``field``, ``op``, and ``value`` keys.
type FilterSpec = StrAnyMap

# Field renaming instructions mapping old keys to new ones.
type MapSpec = StrStrMap

# Projection spec as a field list or mapping with metadata.
#
# Examples
# --------
# >>> from etlplus.ops.types import SelectSpec
# >>> spec1: SelectSpec = ['a','b']
# >>> spec2: SelectSpec = {'fields': [...]}
type SelectSpec = Fields | StrSeqMap

# Sort directive expressed as a field string or mapping with flags.
#
# Examples
# --------
# >>> from etlplus.ops.types import SortSpec
# >>> spec1: SortSpec = 'field'
# >>> spec2: SortSpec = {'field': 'x', 'reverse': True}
type SortSpec = str | StrAnyMap

# Aggregate instruction covering ``field``, ``func``, and optional alias.
#
# Supported functions: ``avg``, ``count``, ``max``, ``min``, and ``sum``.
# Examples
# --------
# >>> from etlplus.ops.types import AggregateSpec
# >>> spec: AggregateSpec = \
# ...   {'field': 'x', 'func': 'sum' | 'avg' | ..., 'alias'?: '...'}
type AggregateSpec = StrAnyMap

# -- Pipelines-- #

# Unified pipeline step spec consumed by :mod:`etlplus.ops.transform`.
type StepSpec = AggregateSpec | FilterSpec | MapSpec | SelectSpec | SortSpec

# Collections of steps

# Ordered collection of :data:`StepSpec` entries.
type StepSeq = Sequence[StepSpec]

# Accepts either a single :data:`StepSpec` or a sequence of them.
type StepOrSteps = StepSpec | StepSeq

# Canonical literal names for supported transform stages.
type PipelineStepName = Literal['aggregate', 'filter', 'map', 'select', 'sort']

# Mapping from step name to its associated specification payload.
# TODO: Consider replacing with etlplus.workflow.types.PipelineConfig.
type PipelineConfig = Mapping[PipelineStepName, StepOrSteps]

# -- Helpers -- #

# Callable that applies step configuration to a batch of records.
type StepApplier = Callable[[JSONList, Any], JSONList]

# Tuple combining stable sort index and computed sort value.
type SortKey = tuple[int, Any]
