"""
:mod:`etlplus.connector._connector` module.

Compatibility re-exports for connector configuration classes.
"""

from __future__ import annotations

from ._api import ConnectorApi
from ._database import ConnectorDb
from ._file import ConnectorFile
from ._queue import ConnectorQueue

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Type Aliases
    'Connector',
]


# SECTION: TYPED ALIASES ==================================================== #


# Type alias representing any supported connector
type Connector = ConnectorApi | ConnectorDb | ConnectorFile | ConnectorQueue
