"""
:mod:`etlplus.connector.types` module.

Type aliases and editor-only :class:`TypedDict`s for :mod:`etlplus.connector`.

These types improve IDE autocomplete and static analysis while the runtime
parsers remain permissive.

Notes
-----
- TypedDicts in this module are intentionally ``total=False`` and are not
    enforced at runtime.
- :meth:`*.from_obj` constructors accept :class:`Mapping[str, Any]` and perform
    tolerant parsing and light casting. This keeps the runtime permissive while
    improving autocomplete and static analysis for contributors.

Examples
--------
>>> from etlplus.connector import Connector
>>> src: Connector = {
>>>     "type": "file",
>>>     "path": "/data/input.csv",
>>> }
>>> tgt: Connector = {
>>>     "type": "database",
>>>     "connection_string": "postgresql://user:pass@localhost/db",
>>> }
>>> from etlplus.api import RetryPolicy
>>> rp: RetryPolicy = {"max_attempts": 3, "backoff": 0.5}
"""

from __future__ import annotations

from typing import Literal

from .enums import DataConnectorType

# SECTION: EXPORTS  ========================================================= #


__all__ = [
    # Type Aliases
    'ConnectorType',
]


# SECTION: TYPE ALIASES ===================================================== #


# Literal type for supported connector kinds (strings or enum members)
type ConnectorType = DataConnectorType | Literal['api', 'database', 'file']
