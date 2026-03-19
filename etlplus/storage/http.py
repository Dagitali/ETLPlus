"""
:mod:`etlplus.storage.http` module.

HTTP(S) storage backend.
"""

from __future__ import annotations

from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import contextmanager
from typing import IO
from typing import Any
from typing import Protocol
from typing import cast

import requests  # type: ignore[import]

from ._remote import RemoteStorageBackend
from ._remote_buffer import open_remote_buffer
from ._remote_buffer import parse_remote_open_mode
from .enums import StorageScheme
from .location import StorageLocation

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'HttpStorageBackend',
]


# SECTION: PROTOCOLS ======================================================== #


class HttpResponseProtocol(Protocol):
    """Protocol for the subset of HTTP response behavior this backend uses."""

    content: bytes
    status_code: int

    def close(self) -> None:
        """Close the response and release any underlying resources."""

    def raise_for_status(self) -> None:
        """Raise an exception when the response indicates failure."""


class HttpSessionProtocol(Protocol):
    """Protocol for the subset of session behavior this backend uses."""

    def close(self) -> None:
        """Close the session and release any underlying resources."""

    def get(
        self,
        url: str,
        *,
        allow_redirects: bool = True,
        headers: Mapping[str, str] | None = None,
        stream: bool = False,
        timeout: float | None = None,
    ) -> HttpResponseProtocol:
        """Return one HTTP GET response object."""

    def head(
        self,
        url: str,
        *,
        allow_redirects: bool = True,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> HttpResponseProtocol:
        """Return one HTTP HEAD response object."""


# SECTION: CLASSES ========================================================== #


class HttpStorageBackend(RemoteStorageBackend):
    """
    Read-only storage backend for ``http://`` and ``https://`` locations.

    Runtime operations use :mod:`requests` and stage remote responses through
    the shared in-memory remote buffer utilities.
    """

    # -- Class Attributes -- #

    authority_label = 'host'
    path_label = 'URL path'
    scheme = StorageScheme.HTTP
    service_name = 'HTTP'

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(
        self,
        *,
        session: HttpSessionProtocol | None = None,
        timeout: float | None = 30.0,
        headers: Mapping[str, str] | None = None,
        allow_redirects: bool = True,
    ) -> None:
        self.session = session
        self.timeout = timeout
        self.headers = dict(headers or {})
        self.allow_redirects = allow_redirects

    # -- Internal Instance Methods -- #

    def _merged_headers(
        self,
        headers: Mapping[str, str] | None,
    ) -> dict[str, str]:
        """Return per-request headers merged with backend defaults."""
        return {**self.headers, **dict(headers or {})}

    def _resolved_timeout(
        self,
        timeout: float | None,
    ) -> float | None:
        """Return the effective timeout for one request."""
        return self.timeout if timeout is None else timeout

    @contextmanager
    def _session_scope(self) -> Iterator[HttpSessionProtocol]:
        """Yield a requests session, closing owned sessions on exit."""
        if self.session is not None:
            yield self.session
            return

        session = cast(HttpSessionProtocol, requests.Session())
        try:
            yield session
        finally:
            session.close()

    # -- Instance Methods -- #

    def delete(
        self,
        location: StorageLocation,
    ) -> None:
        """
        Reject deletion for HTTP resources.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Raises
        ------
        ValueError
            Always, because generic HTTP resources are treated as read-only.
        """
        self._validate(location)
        raise ValueError('HTTP storage backend is read-only; delete is not supported')

    def exists(
        self,
        location: StorageLocation,
    ) -> bool:
        """
        Return whether one HTTP resource exists.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Returns
        -------
        bool
            ``True`` when the remote resource exists.
        """
        self._validate(location)
        headers = self._merged_headers(None)
        timeout = self._resolved_timeout(None)
        with self._session_scope() as session:
            response = session.head(
                location.raw,
                allow_redirects=self.allow_redirects,
                headers=headers,
                timeout=timeout,
            )
            try:
                if response.status_code == 404:
                    return False
                if response.status_code in {405, 501}:
                    fallback = session.get(
                        location.raw,
                        allow_redirects=self.allow_redirects,
                        headers=headers,
                        stream=True,
                        timeout=timeout,
                    )
                    try:
                        if fallback.status_code == 404:
                            return False
                        fallback.raise_for_status()
                        return True
                    finally:
                        fallback.close()
                response.raise_for_status()
                return True
            finally:
                response.close()

    def open(
        self,
        location: StorageLocation,
        mode: str = 'r',
        **kwargs: Any,
    ) -> IO[Any]:
        """
        Open one HTTP resource via an in-memory file-like buffer.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        mode : str, optional
            Remote open mode. Supports only ``r``, ``rb``, and ``rt``.
        **kwargs : Any
            Text-mode options such as ``encoding``, ``errors``, and
            ``newline``. Also accepts ``headers``, ``timeout``, and
            ``allow_redirects``.

        Returns
        -------
        IO[Any]
            In-memory file-like object backed by an HTTP GET response.

        Raises
        ------
        FileNotFoundError
            If the requested HTTP resource does not exist.
        TypeError
            If unsupported keyword arguments are provided.
        ValueError
            If a write mode is requested.
        """
        self._validate(location)
        kind, text_mode = parse_remote_open_mode(mode)
        if kind == 'write':
            raise ValueError(
                'HTTP storage backend is read-only; only r/rb/rt are supported',
            )

        encoding = kwargs.pop('encoding', 'utf-8')
        errors = kwargs.pop('errors', None)
        newline = kwargs.pop('newline', None)
        headers = cast(Mapping[str, str] | None, kwargs.pop('headers', None))
        timeout = cast(float | None, kwargs.pop('timeout', None))
        allow_redirects = cast(
            bool,
            kwargs.pop('allow_redirects', self.allow_redirects),
        )
        if kwargs:
            unexpected = ', '.join(sorted(kwargs))
            raise TypeError(
                f'Unsupported HTTP open() keyword arguments: {unexpected}',
            )

        resolved_headers = self._merged_headers(headers)
        resolved_timeout = self._resolved_timeout(timeout)
        with self._session_scope() as session:
            response = session.get(
                location.raw,
                allow_redirects=allow_redirects,
                headers=resolved_headers,
                timeout=resolved_timeout,
            )
            try:
                if response.status_code == 404:
                    raise FileNotFoundError(f'File not found: {location.raw}')
                response.raise_for_status()
                payload = response.content
            finally:
                response.close()

        return open_remote_buffer(
            kind='read',
            text_mode=text_mode,
            payload=payload,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )
