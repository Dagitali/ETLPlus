"""
:mod:`etlplus.types` module.

Shared type aliases leveraged across ETLPlus modules.

Notes
-----
- Centralizes JSON- and pipeline-oriented aliases to keep modules focused on
    orchestration logic.
- Relies on Python 3.13 ``type`` statements for readability and IDE support.

See Also
--------
- :mod:`etlplus.api.types` for HTTP-specific aliases and data classes
- :mod:`etlplus.connector.types` for connector-specific aliases

Examples
--------
>>> from etlplus.types import JSONDict
>>> from etlplus.ops.types import PipelineConfig
>>> payload: JSONDict = {'id': 1, 'name': 'Ada'}
>>> isinstance(payload, dict)
True
>>> config: PipelineConfig = {
...     'filter': [{'field': 'id', 'op': '>', 'value': 0}],
... }
>>> isinstance(config, dict)
True
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
    # Type Aliases (Data)
    'JSONData',
    'JSONDict',
    'JSONList',
    'JSONScalar',
    'JSONValue',
    'Record',
    'Records',
    'JSONRecord',
    'JSONRecords',
    # Type Aliases (File System)
    'StrPath',
    # Type Aliases (Transform Specs)
    'StrAnyMap',
    'StrSeqMap',
    'StrStrMap',
    # Type Aliases (Networking / Runtime)
    'Sleeper',
    'Timeout',
    # Type Aliases (Templates)
    'TemplateKey',
]


# SECTION: TYPE ALIASES ===================================================== #


# -- Data -- #

# Mapping representing a JSON object keyed by strings.
type JSONDict = dict[str, Any]

# Ordered collection of JSON objects, commonly used for batches.
type JSONList = list[JSONDict]

# Union capturing the primary ETL payload (record or batch).
type JSONData = JSONDict | JSONList

# JSON scalar/value aliases (useful for stricter schemas elsewhere)

# Primitive JSON-compatible value used across validators.
type JSONScalar = None | bool | int | float | str

# Recursive JSON-friendly value supporting nested payloads.
type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]

# Convenience synonyms

# Alias maintained for compatibility with earlier helpers.
type Record = JSONDict

# Synonym for :data:`JSONList` when semantics target record sets.
type Records = JSONList

# Explicit alias favored by API pagination helpers.
type JSONRecord = JSONDict

# List of :data:`JSONRecord` values returned by pagination utilities.
type JSONRecords = list[JSONRecord]

# -- File System -- #

# Path-like inputs accepted by file helpers.
type StrPath = str | Path | PathLike[str]

# -- Transform Specs -- #

# Kept intentionally broad for runtime-friendly validation in transform.py.

# Base building blocks to simplify complex specs.

# Mapping of string keys to arbitrary values.
type StrAnyMap = Mapping[str, Any]

# Mapping constrained to string-to-string transformations.
type StrStrMap = Mapping[str, str]

# Mapping whose values are homogeneous sequences.
type StrSeqMap = Mapping[str, Sequence[Any]]

# -- Networking / Runtime -- #

# Sleep function used by retry helpers.
type Sleeper = Callable[[float], None]

# Numeric timeout in seconds or ``None`` for no timeout.
type Timeout = float | None

# -- Templates -- #

# Allowed template keys for bundled DDL rendering.
type TemplateKey = Literal['ddl', 'view']
