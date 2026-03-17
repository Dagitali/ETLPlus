"""
:mod:`etlplus.storage.abfs` module.

ABFS storage backend skeleton.
"""

from __future__ import annotations

from .enums import StorageScheme
from .stub import StubStorageBackendABC

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AbfsStorageBackend',
]


# SECTION: CLASSES ========================================================== #


class AbfsStorageBackend(StubStorageBackendABC):
    """
    Storage backend skeleton for ``abfs://filesystem@account/path`` locations.

    This backend currently validates ABFS-shaped locations and reserves the
    public runtime surface for a future SDK-backed implementation.
    """

    authority_label = 'filesystem/account authority'
    package_name = 'azure-storage-file-datalake'
    path_label = 'filesystem path'
    scheme = StorageScheme.ABFS
    service_name = 'Azure Data Lake Storage Gen2'
