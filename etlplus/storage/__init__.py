"""
:mod:`etlplus.storage` package.

Storage location and backend helpers.
"""

from __future__ import annotations

from ._base import StorageBackendABC
from ._enums import StorageScheme
from ._location import StorageLocation
from ._registry import coerce_location
from ._registry import get_backend
from ._remote import RemoteStorageBackend
from .abfs import AbfsStorageBackend
from .azure_blob import AzureBlobStorageBackend
from .ftp import FtpStorageBackend
from .http import HttpStorageBackend
from .local import LocalStorageBackend
from .s3 import S3StorageBackend
from .stub import StubStorageBackend

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AbfsStorageBackend',
    'AzureBlobStorageBackend',
    'FtpStorageBackend',
    'HttpStorageBackend',
    'LocalStorageBackend',
    'RemoteStorageBackend',
    'S3StorageBackend',
    'StubStorageBackend',
    'StorageBackendABC',
    'StorageLocation',
    # Enums
    'StorageScheme',
    # Functions
    'coerce_location',
    'get_backend',
]
