"""
etlplus.type module.

Shared type aliases leveraged across ETLPlus modules.
"""
from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from os import PathLike
from pathlib import Path
from typing import Any
from typing import Literal


# SECTION: EXPORTS ========================================================== #


__all__ = [
    'AggregateFunc',
    'FieldName', 'Fields',
    'JSONDict', 'JSONList', 'JSONData', 'JSONScalar', 'JSONValue',
    'JSONRecord', 'JSONRecords',
    'Record', 'Records',
    'OperatorFunc',
    'PipelineStepName', 'PipelineConfig',
    'SortKey',
    'StepApplier', 'StepOrSteps', 'StepSeq', 'StepSpec',
    'StrAnyMap', 'StrPath', 'StrStrMap', 'StrSeqMap',
    'AggregateSpec', 'FilterSpec', 'MapSpec', 'SelectSpec', 'SortSpec',

    # Networking / runtime helpers
    'Timeout',
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
type JSONRecord = JSONDict
type JSONRecords = list[JSONRecord]

# -- File System -- #

type StrPath = str | Path | PathLike[str]

# -- Functions -- #

type AggregateFunc = Callable[[list[float], int], Any]
type OperatorFunc = Callable[[Any, Any], bool]

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

# -- Helpers -- #

type StepApplier = Callable[[JSONList, Any], JSONList]
type SortKey = tuple[int, Any]

# -- Networking / Runtime -- #

# Shared timeout alias for HTTP calls and client configuration.
type Timeout = float | int | None
