"""
:mod:`etlplus.storage.enums` module.

Storage scheme enums and helpers.
"""

from __future__ import annotations

from ..utils.enums import CoercibleStrEnum
from ..utils.types import StrStrMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Enums
    'StorageScheme',
]


# SECTION: ENUMS ============================================================ #


class StorageScheme(CoercibleStrEnum):
    """Supported storage location schemes."""

    # -- Constants -- #

    ABFS = 'abfs'
    AZURE_BLOB = 'azure-blob'
    FILE = 'file'
    FTP = 'ftp'
    S3 = 's3'

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
            'abfss': 'abfs',
            'adls': 'abfs',
            'adls2': 'abfs',
            'azblob': 'azure-blob',
            'azureblob': 'azure-blob',
            'blob': 'azure-blob',
            'filesystem': 'file',
            'local': 'file',
            'fs': 'file',
            's3a': 's3',
            's3n': 's3',
            'wasb': 'azure-blob',
            'wasbs': 'azure-blob',
        }
