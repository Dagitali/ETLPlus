"""
ETLPlus Type Aliases
=======================

Shared type aliases leveraged across ETLPlus modules.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import Callable


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
