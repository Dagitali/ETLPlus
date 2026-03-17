"""
:mod:`etlplus.storage` package.

Storage location and backend helpers.
"""

from __future__ import annotations

from .azure_blob import AzureBlobStorageBackend
from .base import StorageBackend
from .enums import StorageScheme
from .local import LocalStorageBackend
from .location import StorageLocation
from .registry import coerce_location
from .registry import get_backend
from .s3 import S3StorageBackend

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AzureBlobStorageBackend',
    'LocalStorageBackend',
    'S3StorageBackend',
    'StorageBackend',
    'StorageLocation',
    # Enums
    'StorageScheme',
    # Functions
    'coerce_location',
    'get_backend',
]
