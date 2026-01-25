"""
:mod:`etlplus.api.auth` module.

Bearer token authentication for REST APIs using the OAuth2 Client Credentials
flow.

Summary
-------
Use :class:`EndpointCredentialsBearer` with ``requests`` to add
``Authorization: Bearer <token>`` headers. Tokens are fetched and refreshed
on demand with a small clock skew to avoid edge-of-expiry races.

Notes
-----
- Tokens are refreshed when remaining lifetime < ``CLOCK_SKEW_SEC`` seconds.
- Network/HTTP errors are surfaced from ``requests`` with concise logging.

Examples
--------
Basic usage with ``requests.Session``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
>>> from etlplus.api import EndpointCredentialsBearer
>>> auth = EndpointCredentialsBearer(
...     token_url="https://auth.example.com/oauth2/token",
...     client_id="id",
...     client_secret="secret",
...     scope="read",
... )
>>> import requests
>>> s = requests.Session()
>>> s.auth = auth
>>> r = s.get("https://api.example.com/v1/items")
>>> r.raise_for_status()
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any
from typing import Protocol
from typing import TypedDict
from typing import cast

import requests  # type: ignore[import]
from requests import PreparedRequest  # type: ignore
from requests import Response  # type: ignore
from requests.auth import AuthBase  # type: ignore

from .types import Url

logger = logging.getLogger(__name__)


# SECTION: EXPORTS ========================================================== #


__all__ = ['EndpointCredentialsBearer']


# SECTION: CONSTANTS ======================================================== #


CLOCK_SKEW_SEC = 30
DEFAULT_TOKEN_TTL = 3600
DEFAULT_TOKEN_TIMEOUT = 15.0
MAX_LOG_BODY = 500
FORM_HEADERS = MappingProxyType(
    {'Content-Type': 'application/x-www-form-urlencoded'},
)


# SECTION: TYPED DICTS ====================================================== #


class _TokenResponse(TypedDict):
    """Minimal shape of an OAuth token response body."""

    access_token: str
    expires_in: int | float


class _TokenHttpClient(Protocol):
    """Protocol for objects that expose a ``post`` helper like ``requests``."""

    def post(
        self,
        url: Url,
        **kwargs: Any,
    ) -> Response:
        """
        Issue an HTTP POST request and return the response object.

        Parameters
        ----------
        url : Url
            The URL to which the request is sent.
        **kwargs : Any
            Arbitrary request keyword arguments (payload, headers, timeout).

        Returns
        -------
        Response
            HTTP response produced by the client.
        """


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _response_excerpt(
    resp: Response | None,
) -> str:
    """
    Return a short excerpt of ``resp.text`` for diagnostics.

    Parameters
    ----------
    resp : Response | None
        The HTTP response object.

    Returns
    -------
    str
        The first ``MAX_LOG_BODY`` characters of the response body.
    """
    return _truncate(resp.text if resp is not None else '')


def _truncate(
    text: str | None,
    *,
    limit: int = MAX_LOG_BODY,
) -> str:
    """
    Return *text* shortened to *limit* characters for logging.

    Parameters
    ----------
    text : str | None
        The text to truncate.
    limit : int, optional
        The maximum length of the returned string (default is
        ``MAX_LOG_BODY``).

    Returns
    -------
    str
        The truncated text.
    """
    if not text:
        return ''
    return text[:limit]


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True, repr=False, eq=False, kw_only=True)
class EndpointCredentialsBearer(AuthBase):
    """
    Bearer token authentication via the OAuth2 Client Credentials flow.

    Summary
    -------
    Implements ``requests`` ``AuthBase`` to lazily obtain and refresh an
    access token, adding ``Authorization: Bearer <token>`` to outgoing
    requests. A small clock skew avoids edge-of-expiry races.

    Parameters
    ----------
    token_url : str
        OAuth2 token endpoint URL.
    client_id : str
        OAuth2 client ID.
    client_secret : str
        OAuth2 client secret.
    scope : str | None, optional
        Optional OAuth2 scope string.

    Attributes
    ----------
    token_url : str
        OAuth2 token endpoint URL.
    client_id : str
        OAuth2 client ID.
    client_secret : str
        OAuth2 client secret.
    scope : str | None
        Optional OAuth2 scope string.
    token : str | None
        Current access token (``None`` until first successful request).
    expiry : float
        UNIX timestamp when the token expires.
    timeout : float
        Timeout in seconds for token requests (defaults to
        ``DEFAULT_TOKEN_TIMEOUT``).
    session : requests.Session | None
        Optional session used for token requests to leverage connection
        pooling and shared auth state. Falls back to the module-level
        ``requests`` functions when ``None``.

    Notes
    -----
    - Tokens are refreshed when remaining lifetime < ``CLOCK_SKEW_SEC``.
    - Network/HTTP errors propagate as ``requests`` exceptions from
        ``_ensure_token``.
    - Missing ``access_token`` in a successful response raises
        ``RuntimeError``.
    """

    # -- Attributes -- #

    token_url: str
    client_id: str
    client_secret: str
    scope: str | None = None
    token: str | None = None
    expiry: float = 0.0
    timeout: float = DEFAULT_TOKEN_TIMEOUT
    session: requests.Session | None = None

    # -- Magic Methods (Object Behavior) -- #

    def __call__(
        self,
        r: PreparedRequest,
    ) -> PreparedRequest:
        """
        Attach an Authorization header to an outgoing request.

        Ensures a valid access token is available, refreshing when
        necessary, and sets ``Authorization: Bearer <token>`` on the
        provided request object.

        Parameters
        ----------
        r : PreparedRequest
            The request object that will be sent by ``requests``.

        Returns
        -------
        PreparedRequest
            The same request with the Authorization header set.
        """
        self._ensure_token()
        r.headers['Authorization'] = f'Bearer {self.token}'
        return r

    # -- Internal Instance Methods -- #

    def _ensure_token(self) -> None:
        """
        Fetch or refresh the bearer token if expired or missing.

        Uses the OAuth2 Client Credentials flow against ``token_url``.
        Applies a small clock skew to avoid edge-of-expiry races.

        Returns
        -------
        None
            This method mutates ``token`` and ``expiry`` in place.

        Notes
        -----
        Exceptions raised by the underlying HTTP call propagate directly.
        """
        if self._token_valid():
            return

        response = self._request_token()
        self.token = response['access_token']
        ttl = float(response.get('expires_in', DEFAULT_TOKEN_TTL))
        self.expiry = time.time() + max(ttl, 0.0)

    def _http_client(self) -> _TokenHttpClient:
        """Return the configured HTTP session or the module-level client."""
        client = self.session or requests
        return cast(_TokenHttpClient, client)

    def _parse_token_response(
        self,
        resp: Response,
    ) -> _TokenResponse:
        """
        Validate the JSON token response and return a typed mapping.

        Parameters
        ----------
        resp : Response
            The HTTP response from the token endpoint.

        Returns
        -------
        _TokenResponse
            Parsed token response mapping.

        Raises
        ------
        ValueError
            When the response is not valid JSON or not a JSON object.
        RuntimeError
            When the response is missing the ``access_token`` field.
        """
        try:
            payload: Any = resp.json()
        except ValueError:
            logger.error(
                'Token response is not valid JSON. Body: %s',
                _truncate(resp.text),
            )
            raise

        if not isinstance(payload, Mapping):
            logger.error(
                'Token response is not a JSON object (type=%s)',
                type(payload).__name__,
            )
            raise ValueError('Token response must be a JSON object')

        token = payload.get('access_token')
        if not isinstance(token, str) or not token:
            logger.error(
                'Token response missing "access_token". Keys: %s',
                list(payload.keys()),
            )
            raise RuntimeError('Missing access_token in token response')

        raw_ttl = payload.get('expires_in', DEFAULT_TOKEN_TTL)
        try:
            ttl = float(raw_ttl)
        except (TypeError, ValueError):
            ttl = float(DEFAULT_TOKEN_TTL)

        return _TokenResponse(access_token=token, expires_in=ttl)

    def _request_token(self) -> _TokenResponse:
        """
        Execute the OAuth2 token request and parse the response.

        Returns
        -------
        _TokenResponse
            Parsed token response mapping.

        Raises
        ------
        requests.exceptions.Timeout
            On request timeout.
        requests.exceptions.SSLError
            On TLS/SSL errors.
        requests.exceptions.ConnectionError
            On network connection errors.
        requests.exceptions.HTTPError
            On HTTP errors (4xx/5xx responses).
        requests.exceptions.RequestException
            On network/HTTP errors during the token request.
        """
        client = self._http_client()
        try:
            resp = client.post(
                self.token_url,
                data=self._token_payload(),
                auth=(self.client_id, self.client_secret),
                headers=self._token_headers(),
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except requests.exceptions.Timeout:
            logger.error(
                'Token request timed out (url=%s)',
                self.token_url,
            )
            raise
        except requests.exceptions.SSLError:
            logger.error(
                'TLS/SSL error contacting token endpoint (url=%s)',
                self.token_url,
            )
            raise
        except requests.exceptions.ConnectionError:
            logger.error(
                'Network connection error (url=%s)',
                self.token_url,
            )
            raise
        except requests.exceptions.HTTPError as e:
            body = _response_excerpt(e.response)
            code = getattr(e.response, 'status_code', 'N/A')
            logger.error(
                'Token endpoint returned HTTP %s. Body: %s',
                code,
                body,
            )
            raise
        except requests.exceptions.RequestException:
            logger.exception(
                'Unexpected error requesting token (url=%s)',
                self.token_url,
            )
            raise

        return self._parse_token_response(resp)

    def _token_headers(self) -> Mapping[str, str]:
        """Return headers for the token request."""
        return FORM_HEADERS

    def _token_payload(self) -> dict[str, str]:
        """Build the minimal OAuth2 client credentials payload."""
        payload = {
            'grant_type': 'client_credentials',
        }
        if isinstance(self.scope, str) and self.scope.strip():
            payload['scope'] = self.scope
        return payload

    def _token_valid(self) -> bool:
        """
        Return ``True`` when the cached token is usable.

        Returns
        -------
        bool
            ``True`` when a token is present and not expired.
        """
        return self.token is not None and time.time() < (
            self.expiry - CLOCK_SKEW_SEC
        )
