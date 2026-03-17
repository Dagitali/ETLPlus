"""
:mod:`etlplus.storage.registry` module.

Storage backend resolution helpers.
"""

from __future__ import annotations

from functools import cache

from ..utils.types import StrPath
from .azure_blob import AzureBlobStorageBackend
from .base import StorageBackend
from .enums import StorageScheme
from .local import LocalStorageBackend
from .location import StorageLocation
from .s3 import S3StorageBackend

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'coerce_location',
    'get_backend',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


@cache
def _azure_blob_backend() -> AzureBlobStorageBackend:
    """Return the cached Azure Blob storage backend skeleton."""
    return AzureBlobStorageBackend()


@cache
def _local_backend() -> LocalStorageBackend:
    """Return the cached local-storage backend instance."""
    return LocalStorageBackend()


@cache
def _s3_backend() -> S3StorageBackend:
    """Return the cached S3 storage backend skeleton."""
    return S3StorageBackend()


# SECTION: FUNCTIONS ======================================================== #


def coerce_location(
    value: StorageLocation | StrPath | str,
) -> StorageLocation:
    """
    Normalize *value* into a :class:`StorageLocation`.

    Parameters
    ----------
    value : StorageLocation | StrPath | str
        Storage location value.

    Returns
    -------
    StorageLocation
        Parsed location.
    """
    return (
        value
        if isinstance(value, StorageLocation)
        else StorageLocation.from_value(value)
    )


def get_backend(
    value: StorageLocation | StrPath | str,
) -> StorageBackend:
    """
    Resolve a backend for *value*.

    Parameters
    ----------
    value : StorageLocation | StrPath | str
        Storage location value.

    Returns
    -------
    StorageBackend
        Backend capable of serving *value*.

    Raises
    ------
    NotImplementedError
        If the location scheme is recognized but does not yet have a runtime
        backend implementation.
    """
    location = coerce_location(value)
    match location.scheme:
        case StorageScheme.AZURE_BLOB:
            return _azure_blob_backend()
        case StorageScheme.FILE:
            return _local_backend()
        case StorageScheme.S3:
            return _s3_backend()
        case _:
            raise NotImplementedError(
                'Storage backend support is not implemented yet for '
                f'{location.scheme.value!r}',
            )
