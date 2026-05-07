"""
:mod:`etlplus.storage._registry` module.

Storage backend resolution helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import cache

from ..utils._types import StrPath
from ._abfs import AbfsStorageBackend
from ._azure_blob import AzureBlobStorageBackend
from ._base import StorageBackendABC
from ._enums import StorageScheme
from ._ftp import FtpStorageBackend
from ._hdfs import HdfsStorageBackend
from ._http import HttpStorageBackend
from ._local import LocalStorageBackend
from ._location import StorageLocation
from ._s3 import S3StorageBackend

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'coerce_location',
    'get_backend',
]


# SECTION: TYPE ALIASES ===================================================== #


type ValueArg = StorageLocation | StrPath


# SECTION: INTERNAL CONSTANTS =============================================== #


_BACKEND_FACTORIES: dict[StorageScheme, Callable[[], StorageBackendABC]] = {
    StorageScheme.ABFS: AbfsStorageBackend,
    StorageScheme.AZURE_BLOB: AzureBlobStorageBackend,
    StorageScheme.FILE: LocalStorageBackend,
    StorageScheme.FTP: FtpStorageBackend,
    StorageScheme.HDFS: HdfsStorageBackend,
    StorageScheme.HTTP: HttpStorageBackend,
    StorageScheme.S3: S3StorageBackend,
}


# SECTION: INTERNAL FUNCTIONS =============================================== #


@cache
def _backend_for(
    scheme: StorageScheme,
) -> StorageBackendABC:
    """Return the cached backend instance for *scheme*."""
    return _BACKEND_FACTORIES[scheme]()


# SECTION: FUNCTIONS ======================================================== #


def coerce_location(
    value: ValueArg,
) -> StorageLocation:
    """
    Normalize *value* into a :class:`StorageLocation`.

    Parameters
    ----------
    value : ValueArg
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
    value: ValueArg,
) -> StorageBackendABC:
    """
    Resolve a backend for *value*.

    Parameters
    ----------
    value : ValueArg
        Storage location value.

    Returns
    -------
    StorageBackendABC
        Backend capable of serving *value*.

    Raises
    ------
    NotImplementedError
        If the location scheme is recognized but does not yet have a runtime
        backend implementation.
    """
    location = coerce_location(value)
    if (
        isinstance(location.scheme, StorageScheme)
        and location.scheme in _BACKEND_FACTORIES
    ):
        return _backend_for(location.scheme)
    raise NotImplementedError(
        f'Storage backend support is not implemented yet for {location.scheme.value!r}',
    )
