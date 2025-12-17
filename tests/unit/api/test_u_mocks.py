"""
`:mod:`tests.unit.api.test_u_mocks` module.

Reusable mocked objects for API client unit tests.

Notes
-----
- Provides ``MockResponse`` with a simplified ``json`` implementation.
- Provides ``MockSession`` capturing ``get`` calls and close state.
"""

from __future__ import annotations

from typing import Any

import pytest
from requests import Response  # type: ignore[import]
from requests import Session  # type: ignore[import]
from requests.structures import CaseInsensitiveDict  # type: ignore[import]

# SECTION: EXPORTS ========================================================== #


__all__ = ['MockResponse', 'MockSession']


# SECTION: CLASSES ========================================================== #


@pytest.mark.unit
class MockResponse(Response):  # pragma: no cover - behavior trivial
    """
    Minimal ``Response`` subclass returning a provided JSON payload.

    Subclassing ``Response`` keeps the return type compatible for test double
    usage while overriding ``json`` for simplicity.
    """

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(self, payload: Any) -> None:
        super().__init__()
        self._payload = payload
        self.status_code = 200
        self.headers = CaseInsensitiveDict(
            {
                'content-type': 'application/json',
            },
        )

    # -- Instance Methods -- #

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


@pytest.mark.unit
class MockSession(Session):  # pragma: no cover - exercised indirectly
    """
    ``Session`` test double capturing ``get`` calls and close state.

    Notes
    -----
    Captures arguments to ``get`` and tracks close state for test assertions.

    Methods
    -------
    __init__()
        Initializes a MockSession.
    """

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.closed = False

    # -- Instance Methods -- #

    def get(
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
    ) -> Response:  # type: ignore[override]
        """
        Capture ``get`` call arguments and return a simple JSON response.

        Signature mirrors ``requests.Session.get`` to satisfy static type
        checking while keeping implementation intentionally lightweight.
        Unused parameters are accepted for compatibility and recorded only if
        provided.

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
            Whether to follow redirects (default: True).
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
            A MockResponse with a simple JSON payload.
        """
        call_kwargs: dict[str, Any] = {}
        # Persist only explicitly provided (non-None) values for readability.
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
        if allow_redirects is not True:  # only store if deviates default
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
        # Capture any remaining unexpected kwargs for completeness.
        for k, v in kwargs.items():
            if v is not None:
                call_kwargs[k] = v
        self.calls.append((str(url), call_kwargs))
        return MockResponse({'ok': True})

    def close(self) -> None:
        """Mark the session as closed."""
        super().close()
        self.closed = True
