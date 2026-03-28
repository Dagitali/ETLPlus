"""
:mod:`etlplus.connector` package.

Connector configuration types and enums.
"""

from __future__ import annotations

from ._api import ConnectorApi
from ._api import ConnectorApiConfigDict
from ._connector import Connector
from ._core import ConnectorBase
from ._core import ConnectorProtocol
from ._database import ConnectorDb
from ._database import ConnectorDbConfigDict
from ._enums import DataConnectorType
from ._file import ConnectorFile
from ._file import ConnectorFileConfigDict
from ._types import ConnectorType
from ._utils import parse_connector

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
