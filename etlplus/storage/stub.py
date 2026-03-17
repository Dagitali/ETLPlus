"""
:mod:`etlplus.storage.stub` module.

Helpers for intentionally unsupported (stubbed) storage services.
"""

from __future__ import annotations

from typing import IO
from typing import Any
from typing import ClassVar
from typing import Never

from .base import StorageBackendABC
from .enums import StorageScheme
from .location import StorageLocation

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'StubStorageBackendABC',
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
        The storage service being attempted.
    package_name : str
        The package expected to back the future implementation.

    Returns
    -------
    Never

    Raises
    ------
    NotImplementedError
        Always, until the concrete backend is implemented.
    """
    raise NotImplementedError(
        f'{service} storage backend skeleton is present, but runtime '
        f'operations are not wired yet. Add the :mod:`{package_name}`-backed '
        f'implementation before using exists/open against {service}.',
    )


# SECTION: CLASSES ========================================================== #


class StubStorageBackendABC(StorageBackendABC):
    """
    Base class for placeholder storage services.

    Concrete subclasses provide scheme and naming metadata, while this class
    centralizes validation and placeholder runtime behavior.
    """

    # -- Class Attributes -- #

    authority_label: ClassVar[str] = 'service authority'
    package_name: ClassVar[str]
    path_label: ClassVar[str] = 'resource path'
    scheme: ClassVar[StorageScheme]
    service_name: ClassVar[str]

    # -- Internal Instance Methods -- #

    def _raise_not_implemented(
        self,
    ) -> Never:
        """Raise the canonical placeholder error for this service."""
        _raise_not_implemented(
            self.service_name,
            package_name=self.package_name,
        )

    def _validate(
        self,
        location: StorageLocation,
    ) -> None:
        """
        Validate that *location* matches this stub backend.

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

        Stubbed remote backends do not create containers or virtual
        directories, but validating the target early keeps the public surface
        consistent with the local backend.
        """
        self._validate(location)

    def exists(
        self,
        location: StorageLocation,
    ) -> bool:
        """Validate *location* and raise until the backend is implemented."""
        self._validate(location)
        self._raise_not_implemented()

    def open(
        self,
        location: StorageLocation,
        mode: str = 'r',
        **kwargs: Any,
    ) -> IO[Any]:
        """Validate *location* and raise until the backend is implemented."""
        del mode, kwargs
        self._validate(location)
        self._raise_not_implemented()
