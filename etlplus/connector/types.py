"""
:mod:`etlplus.connector.types` module.

Connector type aliases for :mod:`etlplus.connector`.

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
