"""
:mod:`etlplus.connector.connector` module.

Compatibility re-exports for connector configuration classes.
"""

from __future__ import annotations

from .api import ConnectorApi
from .database import ConnectorDb
from .file import ConnectorFile

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Type aliases
    'Connector',
]


# SECTION: TYPED ALIASES ==================================================== #


# Type alias representing any supported connector
type Connector = ConnectorApi | ConnectorDb | ConnectorFile
