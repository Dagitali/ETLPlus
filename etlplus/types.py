"""
ETLPlus Type Aliases
=======================

Shared type aliases leveraged across ETLPlus modules.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any


# SECTION: TYPE ALIASES ===================================================== #


type JSONDict = dict[str, Any]
type JSONList = list[JSONDict]
type JSONData = JSONDict | JSONList
type StrPath = str | Path
