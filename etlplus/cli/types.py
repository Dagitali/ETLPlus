"""
:mod:`etlplus.cli.types` module.

Type aliases for :mod:`etlplus.cli` helpers.

Notes
-----
- Keeps other modules decoupled from ``typing`` details.

Examples
--------
>>> from etlplus.cli.types import DataConnectorContext
>>> connector: DataConnectorContext = 'source'
"""

from __future__ import annotations

from typing import Literal

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Type Aliases
    'DataConnectorContext',
]


# SECTION: TYPE ALIASES ===================================================== #


# Data connector context.
type DataConnectorContext = Literal['source', 'target']
