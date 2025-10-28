"""
ETLPlus Type Aliases
=======================

Shared type aliases leveraged across ETLPlus modules.
"""
from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Literal


# SECTION: TYPE ALIASES ===================================================== #


# -- Data -- #

type JSONDict = dict[str, Any]
type JSONList = list[JSONDict]
type JSONData = JSONDict | JSONList

# -- File System -- #

type StrPath = str | Path

# -- Functions -- #

type OperatorFunc = Callable[[Any, Any], bool]
type AggregateFunc = Callable[[list[float], int], Any]

# -- Operator / Aggregator Names -- #

type OperatorName = \
    Literal['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'in', 'contains']
type Operator = OperatorName | OperatorFunc

type AggregateName = Literal['sum', 'avg', 'min', 'max', 'count']
type Aggregator = AggregateName | AggregateFunc

# -- Records & Fields -- #

type FieldName = str
type Fields = list[FieldName]

# -- Transform Specs -- #

# Kept intentionally broad for runtime-friendly validation in transform.py.

# Expects keys: field, op, value
type FilterSpec = Mapping[str, Any]

# Old_key -> new_key
type MapSpec = Mapping[str, str]


# ['a','b'] or {'fields': [...]}
type SelectSpec = Fields | Mapping[str, Sequence[Any]]

# 'field' or {'field': 'x', 'reverse': True}
type SortSpec = str | Mapping[str, Any]

# {'field': 'x', 'func': 'sum'|'avg'|..., 'alias'?: '...'}
type AggregateSpec = Mapping[str, Any]

type StepSpec = FilterSpec | MapSpec | SelectSpec | SortSpec | AggregateSpec

type PipelineStepName = Literal['filter', 'map', 'select', 'sort', 'aggregate']
type PipelineConfig = Mapping[PipelineStepName, StepSpec | Sequence[StepSpec]]

# -- Helpers -- #

type StepApplier = Callable[[JSONList, Any], JSONList]
type SortKey = tuple[int, Any]
