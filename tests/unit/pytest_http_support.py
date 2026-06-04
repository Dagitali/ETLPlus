"""
:mod:`tests.unit.pytest_http_support` module.

Shared HTTP test doubles for unit tests.
"""

from __future__ import annotations

from typing import Any

from requests import Response  # type: ignore[import]
from requests import Session  # type: ignore[import]
from requests.structures import CaseInsensitiveDict  # type: ignore[import]

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: CLASSES ========================================================== #


class FakeHttpResponse:
    """Minimal HTTP response test double."""

    def __init__(
        self,
        *,
        status_code: int,
        payload: bytes = b'',
    ) -> None:
        self.status_code = status_code
        self.content = payload

    def close(self) -> None:
        """Close the response without side effects."""

    def raise_for_status(self) -> None:
        """Raise one error for non-successful response codes."""
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class FakeHttpSession:
    """Minimal HTTP session double recording HEAD and GET calls."""

    def __init__(
        self,
        *,
        head_status: int = 200,
        get_status: int = 200,
        payload: bytes = b'',
    ) -> None:
        self.calls: list[tuple[str, str, bool]] = []
        self.head_status = head_status
        self.get_status = get_status
        self.payload = payload

    def close(self) -> None:
        """Close the fake session without side effects."""

    def get(self, url: str, **kwargs: Any) -> FakeHttpResponse:
        """Return one fake GET response and capture call metadata."""
        self.calls.append(('get', url, bool(kwargs.get('stream', False))))
        return FakeHttpResponse(
            status_code=self.get_status,
            payload=self.payload,
        )

    def head(self, url: str, **kwargs: Any) -> FakeHttpResponse:
        """Return one fake HEAD response and capture call metadata."""
        self.calls.append(
            ('head', url, bool(kwargs.get('allow_redirects', False))),
        )
        return FakeHttpResponse(status_code=self.head_status)


class MockResponse(Response):  # pragma: no cover - behavior trivial
    """Minimal ``Response`` subclass returning a provided JSON payload."""

    def __init__(self, payload: Any) -> None:
        super().__init__()
        self._payload = payload
        self.status_code = 200
        self.headers = CaseInsensitiveDict({'content-type': 'application/json'})

    def json(
        self,
        **kwargs: Any,
    ) -> Any:
        """
        Return the provided JSON payload.

        Parameters
        ----------
        **kwargs : Any
            Ignored keyword arguments for compatibility.

        Returns
        -------
        Any
            The payload passed to the constructor.
        """
        return self._payload


class MockSession(Session):  # pragma: no cover - exercised indirectly
    """``Session`` test double capturing ``get`` calls and close state."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.closed = False

    def get(  # type: ignore[override]
        self,
        url: str | bytes,
        *,
        params: Any = None,
        data: Any = None,
        headers: Any = None,
        cookies: Any = None,
        files: Any = None,
        auth: Any = None,
        timeout: Any = None,
        allow_redirects: bool = True,
        proxies: Any = None,
        hooks: Any = None,
        stream: Any = None,
        verify: Any = None,
        cert: Any = None,
        json: Any = None,
        **kwargs: Any,
    ) -> Response:
        """
        Capture ``get`` call arguments and return a simple JSON response.

        Parameters
        ----------
        url : str | bytes
            The URL for the GET request.
        params : Any, optional
            Query parameters for the request.
        data : Any, optional
            Data to send in the body of the request.
        headers : Any, optional
            Headers to include in the request.
        cookies : Any, optional
            Cookies to include in the request.
        files : Any, optional
            Files to upload in the request.
        auth : Any, optional
            Authentication credentials.
        timeout : Any, optional
            Timeout for the request.
        allow_redirects : bool, optional
            Whether to follow redirects.
        proxies : Any, optional
            Proxy servers to use for the request.
        hooks : Any, optional
            Event hooks for the request.
        stream : Any, optional
            Whether to stream the response.
        verify : Any, optional
            Whether to verify SSL certificates.
        cert : Any, optional
            Client certificate to use.
        json : Any, optional
            JSON data to send in the request body.
        **kwargs : Any
            Additional keyword arguments.

        Returns
        -------
        Response
            A mock response with a simple JSON payload.
        """
        call_kwargs: dict[str, Any] = {}
        if params is not None:
            call_kwargs['params'] = params
        if data is not None:
            call_kwargs['data'] = data
        if headers is not None:
            call_kwargs['headers'] = headers
        if cookies is not None:
            call_kwargs['cookies'] = cookies
        if files is not None:
            call_kwargs['files'] = files
        if auth is not None:
            call_kwargs['auth'] = auth
        if timeout is not None:
            call_kwargs['timeout'] = timeout
        if allow_redirects is not True:
            call_kwargs['allow_redirects'] = allow_redirects
        if proxies is not None:
            call_kwargs['proxies'] = proxies
        if hooks is not None:
            call_kwargs['hooks'] = hooks
        if stream is not None:
            call_kwargs['stream'] = stream
        if verify is not None:
            call_kwargs['verify'] = verify
        if cert is not None:
            call_kwargs['cert'] = cert
        if json is not None:
            call_kwargs['json'] = json
        call_kwargs.update(
            {key: value for key, value in kwargs.items() if value is not None},
        )
        self.calls.append((str(url), call_kwargs))
        return MockResponse({'ok': True})

    def close(self) -> None:
        """Mark the session as closed."""
        super().close()
        self.closed = True
