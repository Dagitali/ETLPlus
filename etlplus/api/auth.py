"""
ETLPlus API Authorization
======================

REST API helpers for authorization.
"""
from __future__ import annotations

import logging
import time

import requests
from requests.auth import AuthBase

logger = logging.getLogger(__name__)


# SECTION: CLASSES ========================================================= #


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

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(self, token_url, client_id, client_secret, scope=None):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.token = None
        self.expiry = 0

    # -- Magic Methods (Object Behavior) -- #

    def __call__(self, r):
        self._ensure_token()
        r.headers['Authorization'] = f'Bearer {self.token}'
        return r

    # -- Protected Methods -- #

    def _ensure_token(self):
        if self.token and time.time() < self.expiry - 30:
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
