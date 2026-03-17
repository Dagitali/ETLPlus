"""
:mod:`etlplus.storage` package.

Storage location and backend helpers.
"""

from __future__ import annotations

from .abfs import AbfsStorageBackend
from .azure_blob import AzureBlobStorageBackend
from .base import StorageBackendABC
from .enums import StorageScheme
from .ftp import FtpStorageBackend
from .local import LocalStorageBackend
from .location import StorageLocation
from .registry import coerce_location
from .registry import get_backend
from .s3 import S3StorageBackend
from .stub import StubStorageBackendABC

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AbfsStorageBackend',
    'AzureBlobStorageBackend',
    'FtpStorageBackend',
    'LocalStorageBackend',
    'S3StorageBackend',
    'StubStorageBackendABC',
    'StorageBackendABC',
    'StorageLocation',
    # Enums
    'StorageScheme',
    # Functions
    'coerce_location',
    'get_backend',
]
