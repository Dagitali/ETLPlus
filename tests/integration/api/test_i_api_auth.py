"""
:mod:`tests.integration.api.test_i_api_auth` module.

Integration tests for :mod:`etlplus.api.auth` request handshake wiring.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
import requests  # type: ignore[import]

from etlplus.api import EndpointClient
from etlplus.api.auth import EndpointCredentialsBearer
from etlplus.api.errors import ApiAuthError

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


# pylint: disable=import-outside-toplevel,protected-access,unused-argument


def _json_response(
    *,
    url: str,
    payload: Any,
    status: int = 200,
) -> requests.Response:
    """Build a minimal JSON Response for local fake HTTP flows."""
    #
    response = requests.Response()
    response.status_code = status
    response.url = url
    response.headers['content-type'] = 'application/json'
    response._content = json.dumps(payload).encode('utf-8')  # noqa: SLF001
    return response


# SECTION: TESTS ============================================================ #


def test_token_success_handshake_through_auth_and_request_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Token acquisition should authenticate the API request path."""
    token_url = 'https://auth.example.test/oauth/token'
    api_url = 'https://api.example.test/v1/items'
    seen: dict[str, Any] = {
        'token_calls': 0,
        'api_calls': 0,
        'auth_headers': [],
    }

    session = requests.Session()

    def _fake_send(
        request: requests.PreparedRequest,
        **kwargs: Any,  # noqa: ARG001
    ) -> requests.Response:
        if request.url == token_url:
            seen['token_calls'] += 1
            return _json_response(
                url=token_url,
                payload={'access_token': 'token-1', 'expires_in': 120},
            )
        seen['api_calls'] += 1
        seen['auth_headers'].append(request.headers.get('Authorization'))
        return _json_response(url=api_url, payload={'items': [{'id': 1}]})

    monkeypatch.setattr(session, 'send', _fake_send)

    session.auth = EndpointCredentialsBearer(
        token_url=token_url,
        client_id='client-id',
        client_secret='client-secret',
        scope='read',
        session=session,
    )

    client = EndpointClient(
        base_url='https://api.example.test',
        endpoints={'items': '/v1/items'},
        session=session,
    )

    result = client.paginate('items', pagination=None)

    assert result == {'items': [{'id': 1}]}
    assert seen['token_calls'] == 1
    assert seen['api_calls'] == 1
    assert seen['auth_headers'] == ['Bearer token-1']


def test_token_failure_handshake_raises_api_auth_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Token endpoint failures should surface as ApiAuthError on request."""
    token_url = 'https://auth.example.test/oauth/token'

    session = requests.Session()

    def _fake_send(
        request: requests.PreparedRequest,
        **kwargs: Any,  # noqa: ARG001
    ) -> requests.Response:
        if request.url == token_url:
            return _json_response(
                url=token_url,
                payload={'error': 'unauthorized'},
                status=401,
            )
        return _json_response(
            url='https://api.example.test/v1/items',
            payload={'items': []},
        )

    monkeypatch.setattr(session, 'send', _fake_send)

    session.auth = EndpointCredentialsBearer(
        token_url=token_url,
        client_id='client-id',
        client_secret='client-secret',
        session=session,
    )

    client = EndpointClient(
        base_url='https://api.example.test',
        endpoints={'items': '/v1/items'},
        session=session,
    )

    with pytest.raises(ApiAuthError) as exc_info:
        client.paginate('items', pagination=None)

    assert exc_info.value.status == 401
