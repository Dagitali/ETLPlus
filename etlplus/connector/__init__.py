"""
:mod:`etlplus.connector` package.

Connector configuration types and enums.
"""

from __future__ import annotations

from .api import ConnectorApi
from .api import ConnectorApiConfigDict
from .connector import Connector
from .core import ConnectorBase
from .core import ConnectorProtocol
from .database import ConnectorDb
from .database import ConnectorDbConfigDict
from .enums import DataConnectorType
from .file import ConnectorFile
from .file import ConnectorFileConfigDict
from .types import ConnectorType
from .utils import parse_connector

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'ConnectorApi',
    'ConnectorDb',
    'ConnectorFile',
    # Enums
    'DataConnectorType',
    # Functions
    'parse_connector',
    # Type Aliases
    'Connector',
    'ConnectorBase',
    'ConnectorProtocol',
    'ConnectorType',
    # Typed Dicts
    'ConnectorApiConfigDict',
    'ConnectorDbConfigDict',
    'ConnectorFileConfigDict',
]
