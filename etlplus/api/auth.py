"""
etlplus.api.auth
================

A module enabling bearer token authentication for REST APIs using the OAuth2
Client Credentials flow.

Summary
-------
Use :class:`EndpointCredentialsBearer` with ``requests`` to automatically add
``Authorization: Bearer <token>`` headers. Tokens are fetched and refreshed
on demand with a small clock skew to avoid edge-of-expiry races.

Notes
-----
- Tokens are refreshed when the remaining lifetime is less than
    ``CLOCK_SKEW_SEC`` seconds.
- Network and HTTP errors are surfaced from ``requests``; logs provide
    brief diagnostics.

Examples
--------

Basic usage with ``requests.Session``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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

import requests  # type: ignore
from requests.auth import AuthBase  # type: ignore

logger = logging.getLogger(__name__)


# SECTION: CONSTANTS ======================================================== #


CLOCK_SKEW_SEC = 30


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True, repr=False, eq=False)
class EndpointCredentialsBearer(AuthBase):
    """
    Bearer token authentication via OAuth2 client credentials flow.

    Attributes
    ----------
    token_url : str
        The OAuth2 token endpoint URL.
    client_id : str
        The OAuth2 client ID.
    client_secret : str
        The OAuth2 client secret.
    scope : str | None
        Optional OAuth2 scope.
    token : str | None
        The current access token, if obtained.
    expiry : float
        The UNIX timestamp when the token expires.
    """

    # -- Attributes -- #

    token_url: str
    client_id: str
    client_secret: str
    scope: str | None = None
    token: str | None = None
    expiry: float = 0.0

    # -- Magic Methods (Object Behavior) -- #

    def __call__(self, r):
        self._ensure_token()
        r.headers['Authorization'] = f'Bearer {self.token}'
        return r

    # -- Protected Methods -- #

    def _ensure_token(self):
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
