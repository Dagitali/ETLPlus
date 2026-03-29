"""
:mod:`etlplus.storage._location` module.

Parsed storage-location helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
from os import fspath
from pathlib import Path
from urllib.parse import unquote
from urllib.parse import urlsplit

from ..utils._types import StrPath
from ._enums import StorageScheme

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'StorageLocation',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _from_https_azure_url(
    raw: str,
    *,
    authority: str,
    path: str,
) -> StorageLocation | None:
    """Parse Azure HTTPS object URLs into storage-native locations."""
    host = authority.casefold()
    parts = [segment for segment in unquote(path).split('/') if segment]

    if host.endswith('.blob.core.windows.net'):
        container = parts[0] if parts else ''
        blob_path = '/'.join(parts[1:])
        return StorageLocation(
            raw=raw,
            scheme=StorageScheme.AZURE_BLOB,
            path=blob_path,
            authority=(f'{container}@{authority}' if container else f'@{authority}'),
        )

    if host.endswith('.dfs.core.windows.net'):
        file_system = parts[0] if parts else ''
        file_path = '/'.join(parts[1:])
        return StorageLocation(
            raw=raw,
            scheme=StorageScheme.ABFS,
            path=file_path,
            authority=(
                f'{file_system}@{authority}' if file_system else f'@{authority}'
            ),
        )

    return None


def _looks_like_windows_drive(
    raw: str,
) -> bool:
    """Return whether *raw* begins with a Windows drive prefix."""
    return len(raw) >= 2 and raw[0].isalpha() and raw[1] == ':'


# SECTION: CLASSES ========================================================== #


@dataclass(frozen=True, slots=True)
class StorageLocation:
    """
    Parsed storage location.

    Attributes
    ----------
    raw : str
        Original location string or path representation.
    scheme : StorageScheme
        Normalized storage scheme.
    path : str
        Path component within the storage system.
    authority : str
        Optional authority or container host segment.
    """

    # -- Instance Attributes -- #

    raw: str
    scheme: StorageScheme
    path: str
    authority: str = ''

    # -- Class Methods -- #

    @classmethod
    def from_value(
        cls,
        value: StrPath,
    ) -> StorageLocation:
        """
        Parse a path or URI into a :class:`StorageLocation`.

        Parameters
        ----------
        value : StrPath
            Local path-like input or storage URI.

        Returns
        -------
        StorageLocation
            Parsed storage location.

        Raises
        ------
        ValueError
            If *value* is empty.
        """
        raw = fspath(value).strip()
        if not raw:
            raise ValueError('Storage location cannot be empty')

        parsed = urlsplit(raw)
        if parsed.scheme and not _looks_like_windows_drive(raw):
            if parsed.scheme.casefold() == 'https':
                azure_location = _from_https_azure_url(
                    raw,
                    authority=parsed.netloc,
                    path=parsed.path,
                )
                if azure_location is not None:
                    return azure_location

            scheme = StorageScheme.coerce(parsed.scheme)
            if scheme is StorageScheme.FILE:
                local_path = unquote(
                    parsed.path or raw.removeprefix('file://'),
                )
                return cls(
                    raw=raw,
                    scheme=scheme,
                    path=local_path,
                    authority=parsed.netloc,
                )
            remote_path = unquote(parsed.path.lstrip('/'))
            return cls(
                raw=raw,
                scheme=scheme,
                path=remote_path,
                authority=parsed.netloc,
            )

        return cls(
            raw=raw,
            scheme=StorageScheme.FILE,
            path=raw,
            authority='',
        )

    # -- Getters -- #

    @property
    def is_local(self) -> bool:
        """Return whether the location targets local storage."""
        return self.scheme is StorageScheme.FILE

    @property
    def is_remote(self) -> bool:
        """Return whether the location targets non-local storage."""
        return not self.is_local

    # -- Instance Methods -- #

    def as_path(self) -> Path:
        """
        Convert a local storage location to :class:`pathlib.Path`.

        Returns
        -------
        Path
            Local filesystem path.

        Raises
        ------
        TypeError
            If the location does not represent local storage.
        """
        if not self.is_local:
            raise TypeError(
                f'Only local storage locations can be converted to Path; '
                f'got {self.scheme.value!r}',
            )
        return Path(self.path)
