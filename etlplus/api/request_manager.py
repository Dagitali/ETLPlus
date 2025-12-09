"""
:mod:`etlplus.api.request_manager` module.

Helper utilities for handling REST API endpoint request/response helper
utilities.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from functools import partial
from typing import Any
from typing import cast

import requests  # type: ignore[import]
from requests import Response  # type: ignore[import]

from ..types import JSONData
from ..types import JSONDict
from .errors import ApiAuthError
from .errors import ApiRequestError
from .request import RetryManager
from .request import RetryPolicy

# SECTION: CONSTANTS ======================================================== #


_MISSING = object()


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class RequestManager:
    """
    Encapsulate HTTP dispatch, retries, and session lifecycle.

    Parameters
    ----------
    retry : RetryPolicy | None, optional
        Retry policy to apply to requests. Default is ``None``.
    retry_network_errors : bool, optional
        Whether to retry on network errors. Default is ``False``.
    default_timeout : float, optional
        Default timeout for requests in seconds. Default is 10.0.
    session : requests.Session | None, optional
        Optional pre-configured session to use. Default is ``None``.
    session_factory : Callable[[], requests.Session] | None, optional
        Optional factory for creating sessions. Default is ``None``.
    retry_cap : float, optional
        Maximum backoff cap in seconds. Default is 30.0.

    Attributes
    ----------
    retry : RetryPolicy | None
        Retry policy to apply to requests.
    retry_network_errors : bool
        Whether to retry on network errors.
    default_timeout : float
        Default timeout for requests in seconds.
    session : requests.Session | None
        Optional pre-configured session to use.
    session_factory : Callable[[], requests.Session] | None
        Optional factory for creating sessions.
    retry_cap : float
        Maximum backoff cap in seconds.
    """

    # -- Attributes -- #

    retry: RetryPolicy | None = None
    retry_network_errors: bool = False
    default_timeout: float = 10.0
    session: requests.Session | None = None
    session_factory: Callable[[], requests.Session] | None = None
    retry_cap: float = 30.0

    # -- Internal Attributes -- #

    _ctx_session: Any | None = field(default=None, init=False, repr=False)
    _ctx_owns_session: bool = field(default=False, init=False, repr=False)

    # -- Magic Methods (Context Manager Protocol) -- #

    def __enter__(self) -> RequestManager:
        """Enter the runtime context related to this object."""
        if self._ctx_session is not None:
            return self
        if self.session is not None:
            self._ctx_session = self.session
            self._ctx_owns_session = False
            return self
        if self.session_factory is not None:
            sess = self.session_factory()
            self._ctx_session = sess
            self._ctx_owns_session = True
            return self
        sess = requests.Session()
        self._ctx_session = sess
        self._ctx_owns_session = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        """
        Exit the runtime context related to this object.

        Parameters
        ----------
        exc_type : type[BaseException] | None
            Exception type if raised, else ``None``.
        exc : BaseException | None
            Exception instance if raised, else ``None``.
        tb : Any
            Traceback if exception raised, else ``None``.
        """
        if self._ctx_session is None:
            return
        if self._ctx_owns_session:
            try:
                self._ctx_session.close()
            except AttributeError:
                pass
        self._ctx_session = None
        self._ctx_owns_session = False

    # -- Instance Methods -- #

    def get(
        self,
        url: str,
        request_callable: Callable[..., JSONData] | None = None,
        **kwargs: Any,
    ) -> JSONData:
        """
        Perform a GET request.

        Parameters
        ----------
        url : str
            Target URL.
        request_callable : Callable[..., JSONData] | None, optional
            Optional custom request function.
        **kwargs : Any
            Additional keyword arguments for the request.

        Returns
        -------
        JSONData
            Parsed JSON response data.
        """
        return self.request(
            'GET',
            url,
            request_callable=request_callable,
            **kwargs,
        )

    def post(
        self,
        url: str,
        request_callable: Callable[..., JSONData] | None = None,
        **kwargs: Any,
    ) -> JSONData:
        """
        Perform a POST request.

        Parameters
        ----------
        url : str
            Target URL.
        request_callable : Callable[..., JSONData] | None, optional
            Optional custom request function.
        **kwargs : Any
            Additional keyword arguments for the request.

        Returns
        -------
        JSONData
            Parsed JSON response data.
        """
        return self.request(
            'POST',
            url,
            request_callable=request_callable,
            **kwargs,
        )

    def request(
        self,
        method: str,
        url: str,
        *,
        request_callable: Callable[..., JSONData] | None = None,
        **kw: Any,
    ) -> JSONData:
        """
        Perform a request with retries.

        Parameters
        ----------
        method : str
            HTTP method (e.g., 'GET', 'POST').
        url : str
            Target URL.
        request_callable : Callable[..., JSONData] | None, optional
            Optional custom request function.
        **kw : Any
            Additional keyword arguments for the request.

        Returns
        -------
        JSONData
            Parsed JSON response data.

        Raises
        ------
        ApiAuthError
            If an authentication error occurs (HTTP 401 or 403).
        ApiRequestError
            If a non-authentication request error occurs.
        """
        method_normalized = self._normalize_http_method(method)

        call_kwargs = dict(kw)
        supplied_timeout = call_kwargs.pop('timeout', _MISSING)
        timeout = self._resolve_timeout(supplied_timeout)
        user_session = call_kwargs.pop('session', None)
        session, owns_session = self._resolve_session_for_call(user_session)
        fetch = partial(
            self.request_once,
            method_normalized,
            session=session,
            timeout=timeout,
            request_callable=request_callable,
        )

        try:
            policy = self.retry
            if not policy:
                try:
                    return fetch(url, **call_kwargs)
                except requests.RequestException as exc:  # pragma: no cover
                    status = getattr(
                        getattr(exc, 'response', None),
                        'status_code',
                        None,
                    )
                    if status in {401, 403}:
                        raise ApiAuthError(
                            url=url,
                            status=status,
                            attempts=1,
                            retried=False,
                            retry_policy=None,
                            cause=exc,
                        ) from exc
                    raise ApiRequestError(
                        url=url,
                        status=status,
                        attempts=1,
                        retried=False,
                        retry_policy=None,
                        cause=exc,
                    ) from exc

            retry_mgr = RetryManager(
                policy=policy,
                retry_network_errors=self.retry_network_errors,
                cap=self.retry_cap,
            )
            return retry_mgr.run_with_retry(fetch, url, **call_kwargs)
        finally:
            if owns_session and session is not None:
                try:
                    session.close()
                except AttributeError:  # pragma: no cover - defensive
                    pass

    def request_once(
        self,
        method: str,
        url: str,
        *,
        session: requests.Session | None,
        timeout: Any,
        request_callable: Callable[..., JSONData] | None = None,
        **kwargs: Any,
    ) -> JSONData:
        """
        Perform a single request without retries.

        Parameters
        ----------
        method : str
            HTTP method (e.g., 'GET', 'POST').
        url : str
            Target URL.
        session : requests.Session | None
            Optional HTTP session to use.
        timeout : Any
            Timeout for the request.
        request_callable : Callable[..., JSONData] | None, optional
            Optional custom request function.
        **kwargs : Any
            Additional keyword arguments for the request.

        Returns
        -------
        JSONData
            Parsed JSON response data.
        """
        method_normalized = self._normalize_http_method(method)
        if request_callable is not None:
            return request_callable(
                method_normalized,
                url,
                session=session,
                timeout=timeout,
                **kwargs,
            )
        response = self._send_http_request(
            method_normalized,
            url,
            session=session,
            timeout=timeout,
            **kwargs,
        )
        response.raise_for_status()
        return self._parse_response_payload(response)

    # -- Internal Instance Methods -- #

    def _resolve_session_for_call(
        self,
        explicit: requests.Session | None,
    ) -> tuple[requests.Session | None, bool]:
        """
        Determine which session should service the current request.

        Parameters
        ----------
        explicit : requests.Session | None
            Session provided directly by the caller.

        Returns
        -------
        tuple[requests.Session | None, bool]
            Pair of ``(session, owns_session)`` where ``owns_session``
            indicates whether this manager is responsible for closing the
            session after the request completes.
        """
        if explicit is not None:
            return explicit, False
        if self._ctx_session is not None:
            return self._ctx_session, False
        if self.session is not None:
            return self.session, False
        if self.session_factory is not None:
            try:
                return self.session_factory(), True
            except (RuntimeError, TypeError):  # pragma: no cover - defensive
                return None, False
        return None, False

    def _parse_response_payload(
        self,
        response: Response,
    ) -> JSONData:
        """
        Parse the response payload into JSONData.

        Parameters
        ----------
        response : Response
            The HTTP response object.

        Returns
        -------
        JSONData
            Parsed JSON response data.
        """
        content_type = response.headers.get('content-type', '').lower()
        if 'application/json' in content_type:
            try:
                payload: Any = response.json()
            except ValueError:
                return {
                    'content': response.text,
                    'content_type': content_type,
                }
            if isinstance(payload, dict):
                return cast(JSONDict, payload)
            if isinstance(payload, list):
                if all(isinstance(item, dict) for item in payload):
                    return cast(JSONData, payload)
                return [{'value': item} for item in payload]
            return {'value': payload}
        return {
            'content': response.text,
            'content_type': content_type,
        }

    def _resolve_request_callable(
        self,
        session: requests.Session | None,
    ) -> Callable[..., Response]:
        """
        Resolve the request callable from the given session or default to
        :func:`requests.request`.

        Parameters
        ----------
        session : requests.Session | None
            Optional session object to use for the request.

        Returns
        -------
        Callable[..., Response]
            Callable to perform the HTTP request.

        Raises
        ------
        TypeError
            If the provided session does not have a callable 'request' method.
        """
        if session is not None:
            request_callable = getattr(session, 'request', None)
            if callable(request_callable):
                return request_callable
            raise TypeError('Session must expose a callable "request" method')
        return requests.request

    def _resolve_timeout(
        self, timeout: Any,
    ) -> Any:
        """
        Resolve the timeout value, defaulting to the instance's
        ``default_timeout`` if not provided.

        Parameters
        ----------
        timeout : Any
            Supplied timeout value.

        Returns
        -------
        Any
            Resolved timeout value.
        """
        return self.default_timeout if timeout is _MISSING else timeout

    def _send_http_request(
        self,
        method: str,
        url: str,
        *,
        session: requests.Session | None,
        timeout: Any,
        **kwargs: Any,
    ) -> Response:
        """
        Send the actual HTTP request using the specified method and URL.

        Parameters
        ----------
        method : str
            HTTP method to use for the request.
        url : str
            Target URL for the request.
        session : requests.Session | None
            Optional session object to use for the request.
        timeout : Any
            Timeout value for the request.
        **kwargs : Any
            Additional keyword arguments for the request.

        Returns
        -------
        Response
            The HTTP response object.
        """
        call_kwargs = {**kwargs, 'timeout': timeout}
        method_normalized = self._normalize_http_method(method)
        request_callable = self._resolve_request_callable(session)
        return request_callable(method_normalized, url, **call_kwargs)

    # -- Internal Static Methods -- #

    @staticmethod
    def _normalize_http_method(
        method: str | None,
    ) -> str:
        """
        Normalize the HTTP method to uppercase, defaulting to 'GET' if not
        provided.

        Parameters
        ----------
        method : str | None
            HTTP method to normalize.

        Returns
        -------
        str
            Normalized HTTP method.
        """
        candidate = (method or '').strip().upper()
        return candidate or 'GET'
