"""
:mod:`etlplus.storage.stub` module.

Helpers for intentionally unsupported (stubbed) storage services.
"""

from __future__ import annotations

from typing import IO
from typing import Any
from typing import ClassVar
from typing import Never

from .location import StorageLocation
from .remote import RemoteStorageBackend

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'StubStorageBackend',
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
        f'operations are not wired yet. Add the {package_name}-backed '
        f'implementation before using exists/open against {service}.',
    )


# SECTION: CLASSES ========================================================== #


class StubStorageBackend(RemoteStorageBackend):
    """
    Base class for placeholder storage services.

    Concrete subclasses provide runtime package metadata, while this class
    layers placeholder runtime behavior on top of the shared remote backend
    validation.
    """

    # -- Class Attributes -- #

    package_name: ClassVar[str]

    # -- Internal Instance Methods -- #

    def _raise_not_implemented(self) -> Never:
        """Raise the canonical placeholder error for this service."""
        _raise_not_implemented(
            self.service_name,
            package_name=self.package_name,
        )

    def exists(
        self,
        location: StorageLocation,
    ) -> bool:
        """
        Validate *location* and raise until the backend is implemented.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Returns
        -------
        bool
            Never, until the backend is implemented.
        """
        self._validate(location)
        self._raise_not_implemented()

    def open(
        self,
        location: StorageLocation,
        mode: str = 'r',
        **kwargs: Any,
    ) -> IO[Any]:
        """
        Validate *location* and raise until the backend is implemented.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        mode : str, optional
            Remote open mode. Supports ``r``, ``rb``, ``rt``, ``w``,
            ``wb``, and ``wt``.
        **kwargs : Any
            Additional keyword arguments.

        Returns
        -------
        IO[Any]
            Never, until the backend is implemented.
        """
        del mode, kwargs
        self._validate(location)
        self._raise_not_implemented()
