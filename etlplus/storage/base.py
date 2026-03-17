"""
:mod:`etlplus.storage.base` module.

Abstract storage backend contracts.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import IO
from typing import Any

from .location import StorageLocation

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'StorageBackendABC',
]


# SECTION: CLASSES ========================================================== #


class StorageBackendABC(ABC):
    """
    Abstract interface for a storage backend.

    Backends encapsulate the mechanics for locating, opening, and preparing
    storage resources. The initial implementation targets local disk, while
    the interface is intentionally broad enough for remote object or file
    stores.
    """

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def ensure_parent_dir(
        self,
        location: StorageLocation,
    ) -> None:
        """
        Ensure that the parent container for *location* exists.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        """

    @abstractmethod
    def exists(
        self,
        location: StorageLocation,
    ) -> bool:
        """
        Return whether *location* exists.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Returns
        -------
        bool
            ``True`` when the resource exists.
        """

    @abstractmethod
    def open(
        self,
        location: StorageLocation,
        mode: str = 'r',
        **kwargs: Any,
    ) -> IO[Any]:
        """
        Open *location* and return a file-like handle.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        mode : str, optional
            Standard Python file mode.
        **kwargs : Any
            Backend-specific keyword arguments forwarded to the concrete open
            implementation.

        Returns
        -------
        IO[Any]
            Open file-like handle.
        """
