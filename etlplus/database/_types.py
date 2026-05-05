"""
:mod:`etlplus.database._types` module.

Shared type aliases leveraged across :mod:`etlplus.database` modules.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import TypeEngine

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Type Aliases
    'ModelRegistry',
    'TypeFactory',
]


# SECTION: TYPE ALIASES ===================================================== #


# pylint: disable=invalid-name

# Registry mapping fully qualified table names to declarative classes.
type ModelRegistry = dict[str, type[DeclarativeBase]]

# Callable producing a SQLAlchemy TypeEngine from parsed parameters.
type TypeFactory = Callable[[Sequence[int]], TypeEngine]
