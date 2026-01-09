"""
:mod:`etlplus.database.types` module.

Shared type aliases leveraged across :mod:`etlplus.database` modules.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import TypeEngine

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Type Aliases
    'ModelRegistry',
    'TemplateKey',
    'TypeFactory',
]


# SECTION: TYPE ALIASES ===================================================== #


# pylint: disable=invalid-name

# Registry mapping fully qualified table names to declarative classes.
type ModelRegistry = dict[str, type[DeclarativeBase]]

# Allowed template keys for bundled DDL rendering.
type TemplateKey = Literal['ddl', 'view']

# Callable producing a SQLAlchemy TypeEngine from parsed parameters.
type TypeFactory = Callable[[list[int]], TypeEngine]
