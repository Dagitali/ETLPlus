"""
:mod:`etlplus.storage.s3` module.

AWS S3 storage backend skeleton.
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
    'S3StorageBackend',
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
        The storage service being attempted (e.g., 'S3').
    package_name : str
        The name of the required package for the storage backend (e.g.,
        'boto3').

    Returns
    -------
    Never

    Raises
    ------
    NotImplementedError
        Always, until the S3 open support is implemented.
    """
    raise NotImplementedError(
        f'{service} storage backend skeleton is present, but runtime '
        f'operations are not wired yet. Add the :mod:`{package_name}`-backed '
        f'implementation before using exists/open against {service}.',
    )


# SECTION: CLASSES ========================================================== #


class S3StorageBackend(StorageBackend):
    """
    Storage backend skeleton for ``s3://bucket/key`` locations.

    This backend currently validates S3-shaped locations and reserves the
    public runtime surface for a future SDK-backed implementation.
    """

    # -- Class Attributes -- #

    scheme = StorageScheme.S3

    # -- Internal Instance Methods -- #

    def _validate(
        self,
        location: StorageLocation,
    ) -> None:
        """
        Validate that *location* is a usable S3 object location.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Raises
        ------
        TypeError
            If *location* does not use the S3 scheme.
        ValueError
            If the bucket or object key is missing.
        """
        if location.scheme is not self.scheme:
            raise TypeError(
                'S3StorageBackend only supports '
                f'{self.scheme.value!r} locations, got '
                f'{location.scheme.value!r}',
            )
        if not location.authority:
            raise ValueError('S3 locations require a bucket name')
        if not location.path:
            raise ValueError('S3 locations require an object key')

    # -- Instance Methods -- #

    def ensure_parent_dir(
        self,
        location: StorageLocation,
    ) -> None:
        """
        Validate *location* and treat parent preparation as a no-op.

        S3 object keys do not require real directory creation, but write paths
        still need structural validation.

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
        Validate *location* and raise until S3 existence checks land.

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
        _raise_not_implemented('S3', package_name='boto3')

    def open(
        self,
        location: StorageLocation,
        mode: str = 'r',
        **kwargs: Any,
    ) -> IO[Any]:
        """
        Validate *location* and raise until S3 open support lands.

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
        _raise_not_implemented('S3', package_name='boto3')
