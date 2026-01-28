"""
:mod:`etlplus.connectors.types` module.

Type aliases and editor-only :class:`TypedDict`s for :mod:`etlplus.connectors`.

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
>>> from etlplus.connectors import Connector
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
from typing import TypedDict

from ..api import PaginationConfigMap
from ..api import RateLimitConfigMap
from ..types import StrAnyMap

# SECTION: EXPORTS  ========================================================= #


__all__ = [
    # Type Aliases
    'ConnectorType',
    # Typed Dicts
    'ConnectorApiConfigMap',
    'ConnectorDbConfigMap',
    'ConnectorFileConfigMap',
]


# SECTION: TYPE ALIASES ===================================================== #


# Literal type for supported connector kinds
type ConnectorType = Literal['api', 'database', 'file']


# SECTION: TYPED DICTS ====================================================== #


class ConnectorApiConfigMap(TypedDict, total=False):
    """
    Shape accepted by :meth:`ConnectorApi.from_obj` (all keys optional).

    See Also
    --------
    - :meth:`etlplus.connectors.connector.ConnectorApi.from_obj`
    """

    name: str
    type: ConnectorType
    url: str
    method: str
    headers: StrAnyMap
    query_params: StrAnyMap
    pagination: PaginationConfigMap
    rate_limit: RateLimitConfigMap
    api: str
    endpoint: str


class ConnectorDbConfigMap(TypedDict, total=False):
    """
    Shape accepted by :meth:`ConnectorDb.from_obj` (all keys optional).

    See Also
    --------
    - :meth:`etlplus.connectors.connector.ConnectorDb.from_obj`
    """

    name: str
    type: ConnectorType
    connection_string: str
    query: str
    table: str
    mode: str


class ConnectorFileConfigMap(TypedDict, total=False):
    """
    Shape accepted by :meth:`ConnectorFile.from_obj` (all keys optional).

    See Also
    --------
    - :meth:`etlplus.connectors.connector.ConnectorFile.from_obj`
    """

    name: str
    type: ConnectorType
    format: str
    path: str
    options: StrAnyMap
