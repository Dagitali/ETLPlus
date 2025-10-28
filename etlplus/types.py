"""
ETLPlus Type Aliases
=======================

Shared type aliases leveraged across ETLPlus modules.
"""
from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from os import PathLike
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Literal

from .enums import AggregateName
from .enums import OperatorName
from .enums import PipelineStep


# SECTION: PUBLIC API ======================================================= #


__all__ = [
    'JSONDict', 'JSONList', 'JSONData', 'JSONScalar', 'JSONValue',
    'Record', 'Records',
    'StrPath',
    'OperatorFunc', 'AggregateFunc',
    'OperatorName', 'Operator', 'AggregateName', 'Aggregator',
    'FieldName', 'Fields',
    'StrAnyMap', 'StrStrMap', 'StrSeqMap',
    'FilterSpec', 'MapSpec', 'SelectSpec', 'SortSpec', 'AggregateSpec',
    'StepSpec', 'StepSeq', 'StepOrSteps',
    'PipelineStepName', 'PipelineConfig', 'PipelinePlan',
    'StepApplier', 'SortKey',
]


# SECTION: TYPE ALIASES ===================================================== #


# -- Data -- #

type JSONDict = dict[str, Any]
type JSONList = list[JSONDict]
type JSONData = JSONDict | JSONList

# JSON scalar/value aliases (useful for stricter schemas elsewhere)
type JSONScalar = None | bool | int | float | str
type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]

# Convenience synonyms
type Record = JSONDict
type Records = JSONList

# -- File System -- #

type StrPath = str | Path | PathLike[str]

# -- Functions -- #

type OperatorFunc = Callable[[Any, Any], bool]
type AggregateFunc = Callable[[list[float], int], Any]

# -- Operator / Aggregator Names -- #

type Aggregator = AggregateName | AggregateFunc
type Operator = OperatorName | OperatorFunc

# -- Records & Fields -- #

type FieldName = str
type Fields = list[FieldName]

# -- Transform Specs -- #

# Kept intentionally broad for runtime-friendly validation in transform.py.

# Base building blocks to simplify complex specs
type StrAnyMap = Mapping[str, Any]
type StrStrMap = Mapping[str, str]
type StrSeqMap = Mapping[str, Sequence[Any]]

type FilterSpec = StrAnyMap  # Expects keys: field, op, value
type MapSpec = StrStrMap  # Old_key -> new_key
type SelectSpec = Fields | StrSeqMap  # ['a','b'] | {'fields': [...]}
type SortSpec = str | StrAnyMap  # 'field' | {'field': 'x', 'reverse': True}

# {'field': 'x', 'func': 'sum' | 'avg' | ..., 'alias'?: '...'}
type AggregateSpec = StrAnyMap

type StepSpec = FilterSpec | MapSpec | SelectSpec | SortSpec | AggregateSpec

# Collections of steps
type StepSeq = Sequence[StepSpec]
type StepOrSteps = StepSpec | StepSeq

type PipelineStepName = Literal['filter', 'map', 'select', 'sort', 'aggregate']
type PipelineConfig = Mapping[PipelineStepName, StepOrSteps]
# Enum-keyed plan (internal use)
type PipelinePlan = Mapping[PipelineStep, StepOrSteps]

# -- Helpers -- #

type StepApplier = Callable[[JSONList, Any], JSONList]
type SortKey = tuple[int, Any]
