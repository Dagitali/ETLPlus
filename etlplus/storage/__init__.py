"""
:mod:`etlplus.storage` package.

Storage location and backend helpers.
"""

from __future__ import annotations

from ._abfs import AbfsStorageBackend
from ._azure_blob import AzureBlobStorageBackend
from ._base import StorageBackendABC
from ._enums import StorageScheme
from ._ftp import FtpStorageBackend
from ._hdfs import HdfsStorageBackend
from ._http import HttpStorageBackend
from ._local import LocalStorageBackend
from ._location import StorageLocation
from ._registry import coerce_location
from ._registry import get_backend
from ._remote import RemoteStorageBackend
from ._s3 import S3StorageBackend
from ._stub import StubStorageBackend

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AbfsStorageBackend',
    'AzureBlobStorageBackend',
    'FtpStorageBackend',
    'HdfsStorageBackend',
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
