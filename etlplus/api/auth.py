"""
etlplus.api.auth module.

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
from dataclasses import dataclass

import requests  # type: ignore[import]
from requests import PreparedRequest  # type: ignore
from requests.auth import AuthBase  # type: ignore

logger = logging.getLogger(__name__)


# SECTION: CONSTANTS ======================================================== #


CLOCK_SKEW_SEC = 30


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True, repr=False, eq=False)
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

    # -- Magic Methods (Object Behavior) -- #

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
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

    # -- Protected Methods -- #

    def _ensure_token(self) -> None:
        """
        Fetch or refresh the bearer token if expired or missing.

        Uses the OAuth2 Client Credentials flow against ``token_url``.
        Applies a small clock skew to avoid edge-of-expiry races.

        Returns
        -------
        None
            This method mutates ``token`` and ``expiry`` in place.

        Raises
        ------
        requests.exceptions.RequestException
            On generic request-level failures.
        requests.exceptions.Timeout
            When the token request times out.
        requests.exceptions.ConnectionError
            On network connection issues.
        requests.exceptions.SSLError
            On TLS/SSL negotiation failures.
        requests.exceptions.HTTPError
            When the endpoint returns a non-2xx status and ``raise_for_status``
            triggers.
        RuntimeError
            When the token response does not include ``access_token``.
        ValueError
            When the token response body is not valid JSON.
        """
        if self.token and time.time() < self.expiry - CLOCK_SKEW_SEC:
            return
        try:
            resp = requests.post(
                self.token_url,
                data={
                    'grant_type': 'client_credentials',
                    'scope': self.scope or '',
                },
                auth=(self.client_id, self.client_secret),
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=15,
            )
            resp.raise_for_status()
        except requests.exceptions.Timeout:
            logger.error(
                'Token request timed out (url=%s)', self.token_url,
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
            body = (e.response.text or '')[:500] or ''
            code = e.response.status_code or 'N/A'
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

        try:
            data = resp.json()
        except ValueError:
            logger.error(
                'Token response is not valid JSON. Body: %s',
                resp.text[:500],
            )
            raise

        tok = data.get('access_token')
        if not tok:
            logger.error(
                'Token response missing "access_token". Keys: %s',
                list(data.keys()),
            )
            raise RuntimeError('Missing access_token in token response')

        self.token = tok
        self.expiry = time.time() + int(data.get('expires_in', 3600))
