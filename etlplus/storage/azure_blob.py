"""
:mod:`etlplus.storage.azure_blob` module.

Azure Blob Storage backend skeleton.
"""

from __future__ import annotations

from typing import IO
from typing import Any
from typing import Never

from .base import StorageBackend
from .enums import StorageScheme
from .location import StorageLocation

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AzureBlobStorageBackend',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _raise_not_implemented(
    service: str,
    *,
    package_name: str,
) -> Never:
    """
    Raise standardized placeholder :class:`NotImplementedError` messages.

    Parameters
    ----------
    service : str
        The storage service being attempted (e.g., 'Azure Blob').
    package_name : str
        The name of the required package for the storage backend (e.g.,
        'azure-storage-blob').

    Returns
    -------
    Never

    Raises
    ------
    NotImplementedError
        Always, until the Azure Blob open support is implemented.
    """
    raise NotImplementedError(
        f'{service} storage backend skeleton is present, but runtime '
        f'operations are not wired yet. Add the :mod:`{package_name}`-backed '
        f'implementation before using exists/open against {service}.',
    )


# SECTION: CLASSES ========================================================== #


class AzureBlobStorageBackend(StorageBackend):
    """
    Storage backend skeleton for Azure Blob object locations.

    The canonical URI form for this surface is
    ``azure-blob://container/blob/path``. The implementation intentionally
    stops at validation until the Azure SDK-backed runtime layer is added.
    """

    # -- Class Attributes -- #

    scheme = StorageScheme.AZURE_BLOB

    # -- Internal Instance Methods -- #

    def _validate(
        self,
        location: StorageLocation,
    ) -> None:
        """
        Validate that *location* is a usable Azure Blob object location.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Raises
        ------
        TypeError
            If *location* does not use the Azure Blob scheme.
        ValueError
            If the container or blob path is missing.
        """
        if location.scheme is not self.scheme:
            raise TypeError(
                'AzureBlobStorageBackend only supports '
                f'{self.scheme.value!r} locations, got '
                f'{location.scheme.value!r}',
            )
        if not location.authority:
            raise ValueError('Azure Blob locations require a container name')
        if not location.path:
            raise ValueError('Azure Blob locations require a blob path')

    # -- Instance Methods -- #

    def ensure_parent_dir(
        self,
        location: StorageLocation,
    ) -> None:
        """
        Validate *location* and treat parent preparation as a no-op.

        Azure Blob uses virtual path segments within a container, so no real
        directory creation occurs at this layer.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        """
        self._validate(location)

    def exists(
        self,
        location: StorageLocation,
    ) -> bool:
        """
        Validate *location* and raise until Azure Blob checks land.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Returns
        -------
        bool
            Whether the S3 object exists.
        """
        self._validate(location)
        _raise_not_implemented('Azure Blob', package_name='azure-storage-blob')

    def open(
        self,
        location: StorageLocation,
        mode: str = 'r',
        **kwargs: Any,
    ) -> IO[Any]:
        """
        Validate *location* and raise until Azure Blob open support lands.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        mode : str, optional
            File mode for opening the S3 object. Defaults to 'r' for read
            access. Write modes are expected to be supported in the future, but
            all modes are currently accepted as part of the placeholder
            surface.
        **kwargs : Any
            Additional keyword arguments for future S3 open implementation.

        Returns
        -------
        IO[Any]
            File-like object for the S3 object.
        """
        del mode, kwargs
        self._validate(location)
        _raise_not_implemented('Azure Blob', package_name='azure-storage-blob')
