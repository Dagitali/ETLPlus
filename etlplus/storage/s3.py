"""
:mod:`etlplus.storage.s3` module.

AWS S3 storage backend skeleton.
"""

from __future__ import annotations

from .enums import StorageScheme
from .stub import StubStorageBackendABC

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'S3StorageBackend',
]


# SECTION: CLASSES ========================================================== #


class S3StorageBackend(StubStorageBackendABC):
    """
    Storage backend skeleton for ``s3://bucket/key`` locations.

    This backend currently validates S3-shaped locations and reserves the
    public runtime surface for a future SDK-backed implementation.
    """

    # -- Class Attributes -- #

    authority_label = 'bucket name'
    package_name = 'boto3'
    path_label = 'object key'
    scheme = StorageScheme.S3
    service_name = 'S3'
