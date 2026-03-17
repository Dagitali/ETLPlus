"""
:mod:`etlplus.storage.base` module.

Abstract storage backend contracts.
"""

from __future__ import annotations

from typing import ClassVar

from .base import StorageBackendABC
from .enums import StorageScheme
from .location import StorageLocation

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RemoteStorageBackend',
]


# SECTION: CLASSES ========================================================== #


class RemoteStorageBackend(StorageBackendABC):
    """
    Shared base class for remote storage backends.

    Concrete subclasses provide scheme and naming metadata, while this class
    centralizes remote-location validation and treats parent preparation as a
    no-op for object or service-backed storage.
    """

    # -- Class Attributes -- #

    authority_label: ClassVar[str] = 'service authority'
    path_label: ClassVar[str] = 'resource path'
    scheme: ClassVar[StorageScheme]
    service_name: ClassVar[str]

    # -- Internal Instance Methods -- #

    def _validate(
        self,
        location: StorageLocation,
    ) -> None:
        """
        Validate that *location* matches this remote backend.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Raises
        ------
        TypeError
            If *location* uses a different scheme.
        ValueError
            If required authority or path components are missing.
        """
        if location.scheme is not self.scheme:
            raise TypeError(
                f'{type(self).__name__} only supports '
                f'{self.scheme.value!r} locations, got '
                f'{location.scheme.value!r}',
            )
        if not location.authority:
            raise ValueError(
                f'{self.service_name} locations require a {self.authority_label}',
            )
        if not location.path:
            raise ValueError(
                f'{self.service_name} locations require a {self.path_label}',
            )

    # -- Instance Methods -- #

    def ensure_parent_dir(
        self,
        location: StorageLocation,
    ) -> None:
        """
        Validate *location* and treat parent preparation as a no-op.

        Remote object stores do not create filesystem-style parent
        directories, but validating the target early keeps the public surface
        consistent across backends.
        """
        self._validate(location)
