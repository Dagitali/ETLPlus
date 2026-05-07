"""
:mod:`etlplus.storage._hdfs` module.

HDFS storage backend.
"""

from __future__ import annotations

import posixpath
from importlib import import_module
from typing import IO
from typing import Any
from typing import cast
from urllib.parse import urlsplit

from ._enums import StorageScheme
from ._location import StorageLocation
from ._remote import RemoteStorageBackend

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'HdfsStorageBackend',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _import_fsspec() -> Any:
    """
    Import and return the :mod:`fsspec` module.

    Returns
    -------
    Any
        Imported fsspec module.

    Raises
    ------
    ImportError
        If ``fsspec`` is not installed.
    """
    try:
        return import_module('fsspec')
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'HDFS storage support requires optional dependency "fsspec".\n'
            'Install with: pip install fsspec',
        ) from e


def _parent_uri(
    location: StorageLocation,
) -> str | None:
    """
    Return the HDFS URI for *location*'s parent directory.

    Parameters
    ----------
    location : StorageLocation
        Parsed HDFS location.

    Returns
    -------
    str | None
        Parent URI, or ``None`` when the location has no parent path.
    """
    parent = posixpath.dirname(location.path)
    if not parent:
        return None
    protocol = urlsplit(location.raw).scheme or location.scheme.value
    return f'{protocol}://{location.authority}/{parent}'


# SECTION: CLASSES ========================================================== #


class HdfsStorageBackend(RemoteStorageBackend):
    """
    Storage backend for ``hdfs://namenode/path`` locations.

    Runtime operations use ``fsspec`` so deployments can rely on the HDFS
    implementation available in the local environment.
    """

    # -- Class Attributes -- #

    authority_label = 'NameNode authority'
    package_name = 'fsspec'
    path_label = 'HDFS path'
    scheme = StorageScheme.HDFS
    service_name = 'HDFS'

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(
        self,
        **storage_options: Any,
    ) -> None:
        self.storage_options = dict(storage_options)

    # -- Internal Instance Methods -- #

    def _filesystem(
        self,
        location: StorageLocation,
    ) -> Any:
        """Return one fsspec filesystem instance for *location*."""
        protocol = urlsplit(location.raw).scheme or location.scheme.value
        fsspec = _import_fsspec()
        return cast(Any, fsspec.filesystem(protocol, **self.storage_options))

    # -- Instance Methods -- #

    def delete(
        self,
        location: StorageLocation,
    ) -> None:
        """
        Delete one HDFS path.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        """
        self._validate(location)
        self._filesystem(location).rm(location.raw)

    def ensure_parent_dir(
        self,
        location: StorageLocation,
    ) -> None:
        """
        Ensure one HDFS parent directory exists.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        """
        self._validate(location)
        parent = _parent_uri(location)
        if parent is not None:
            self._filesystem(location).mkdirs(parent, exist_ok=True)

    def exists(
        self,
        location: StorageLocation,
    ) -> bool:
        """
        Return whether one HDFS path exists.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Returns
        -------
        bool
            ``True`` when the path exists.
        """
        self._validate(location)
        return bool(self._filesystem(location).exists(location.raw))

    def open(
        self,
        location: StorageLocation,
        mode: str = 'r',
        **kwargs: Any,
    ) -> IO[Any]:
        """
        Open one HDFS path through fsspec.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        mode : str, optional
            File mode forwarded to fsspec.
        **kwargs : Any
            Keyword arguments forwarded to ``fsspec.open``.

        Returns
        -------
        IO[Any]
            HDFS-backed file-like handle.
        """
        self._validate(location)
        fsspec = _import_fsspec()
        opener = fsspec.open(
            location.raw,
            mode=mode,
            **self.storage_options,
            **kwargs,
        )
        return cast(IO[Any], opener.open())
