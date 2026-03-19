"""
:mod:`etlplus.connector` package.

Connector configuration types and enums.
"""

from __future__ import annotations

from ._connector import Connector
from ._core import ConnectorBase
from ._core import ConnectorProtocol
from ._utils import parse_connector
from .api import ConnectorApi
from .api import ConnectorApiConfigDict
from .database import ConnectorDb
from .database import ConnectorDbConfigDict
from .enums import DataConnectorType
from .file import ConnectorFile
from .file import ConnectorFileConfigDict
from .types import ConnectorType

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
