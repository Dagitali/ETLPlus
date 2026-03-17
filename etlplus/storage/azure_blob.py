"""
:mod:`etlplus.storage.azure_blob` module.

Azure Blob Storage backend skeleton.
"""

from __future__ import annotations

from .enums import StorageScheme
from .stub import StubStorageBackendABC

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AzureBlobStorageBackend',
]


# SECTION: CLASSES ========================================================== #


class AzureBlobStorageBackend(StubStorageBackendABC):
    """
    Storage backend skeleton for Azure Blob object locations.

    The canonical URI form for this surface is
    ``azure-blob://container/blob/path``. The implementation intentionally
    stops at validation until the Azure SDK-backed runtime layer is added.
    """

    # -- Class Attributes -- #

    authority_label = 'container name'
    package_name = 'azure-storage-blob'
    path_label = 'blob path'
    scheme = StorageScheme.AZURE_BLOB
    service_name = 'Azure Blob'
