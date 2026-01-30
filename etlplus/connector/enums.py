"""
:mod:`etlplus.connector.enums` module.

Connector enums and helpers.
"""

from __future__ import annotations

from ..enums import CoercibleStrEnum
from ..types import StrStrMap

# SECTION: EXPORTS ========================================================= #


__all__ = [
    # Enums
    'DataConnectorType',
]


# SECTION: ENUMS ============================================================ #


class DataConnectorType(CoercibleStrEnum):
    """Supported data connector types."""

    # -- Constants -- #

    API = 'api'
    DATABASE = 'database'
    FILE = 'file'

    # -- Class Methods -- #

    @classmethod
    def aliases(cls) -> StrStrMap:
        """
        Return a mapping of common aliases for each enum member.

        Returns
        -------
        StrStrMap
            A mapping of alias names to their corresponding enum member names.
        """
        return {
            'http': 'api',
            'https': 'api',
            'rest': 'api',
            'db': 'database',
            'filesystem': 'file',
            'fs': 'file',
        }
