"""
:mod:`etlplus.storage._local` module.

Local-disk storage backend.
"""

from __future__ import annotations

from pathlib import Path
from typing import IO
from typing import Any

from ._base import StorageBackendABC
from ._location import StorageLocation

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'LocalStorageBackend',
]


# SECTION: CLASSES ========================================================== #


class LocalStorageBackend(StorageBackendABC):
    """Storage backend for local filesystem paths and ``file://`` URIs."""

    # -- Internal Instance Methods -- #

    def _path(
        self,
        location: StorageLocation,
    ) -> Path:
        """
        Resolve a local :class:`pathlib.Path` from *location*.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Returns
        -------
        Path
            Local path for *location*.
        """
        return location.as_path()

    # -- Instance Methods -- #

    def ensure_parent_dir(
        self,
        location: StorageLocation,
    ) -> None:
        """Ensure that the parent directory for *location* exists."""
        self._path(location).parent.mkdir(parents=True, exist_ok=True)

    def delete(
        self,
        location: StorageLocation,
    ) -> None:
        """Delete *location* from local disk when present."""
        path = self._path(location)
        if path.is_dir():
            path.rmdir()
            return
        path.unlink(missing_ok=True)

    def exists(
        self,
        location: StorageLocation,
    ) -> bool:
        """
        Return whether *location* exists on local disk.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Returns
        -------
        bool
            ``True`` when the resource exists on local disk.
        """
        return self._path(location).exists()

    def open(
        self,
        location: StorageLocation,
        mode: str = 'r',
        **kwargs: Any,
    ) -> IO[Any]:
        """
        Open *location* using :meth:`pathlib.Path.open`.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        mode : str, optional
            Standard Python file mode.
        **kwargs : Any
            Keyword arguments forwarded to :meth:`Path.open`.

        Returns
        -------
        IO[Any]
            Open file-like handle.
        """
        if any(flag in mode for flag in ('w', 'a', 'x', '+')):
            self.ensure_parent_dir(location)
        return self._path(location).open(mode, **kwargs)
