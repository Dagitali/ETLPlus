"""
URL composition with base path
==============================

Verify that when a client is constructed with a base URL that includes a
path prefix (e.g., an API's effective base URL with base_path), calling
``paginate(endpoint_key, ...)`` builds URLs that include that path.

This is a pure URL-building test; network calls are mocked by patching the
module-level ``_extract`` function used by ``EndpointClient``.
"""
from __future__ import annotations

from typing import Any

import etlplus.api.client as cmod
from etlplus.api import EndpointClient


def test_client_base_path_prefixes_endpoint_path(monkeypatch):
    """EndpointClient.url should honor the client's base_path prefix."""

    captured: list[str] = []

    def fake_extract(kind: str, url: str, **_kwargs: Any):
        assert kind == 'api'
        captured.append(url)
        return {'ok': True}

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    # base_url has no path; base_path should be prefixed
    client = EndpointClient(
        base_url='https://api.example.com',
        endpoints={'list': 'items'},
        base_path='/v2',
    )

    out = client.paginate('list', pagination=None)
    assert out == {'ok': True}
    assert captured == ['https://api.example.com/v2/items']


def test_client_base_url_path_and_base_path_both_applied(monkeypatch):
    """Both base_url path and base_path should compose in order."""

    captured: list[str] = []

    def fake_extract(kind: str, url: str, **_kwargs: Any):
        assert kind == 'api'
        captured.append(url)
        return {'ok': True}

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    client = EndpointClient(
        base_url='https://api.example.com/api',
        endpoints={'list': '/items'},
        base_path='v1',  # missing leading slash is tolerated
    )

    client.paginate('list', pagination=None)
    assert captured == ['https://api.example.com/api/v1/items']


def test_url_includes_base_path_when_paginating(monkeypatch):
    """EndpointClient.paginate should include base path from base_url."""

    captured: list[str] = []

    def fake_extract(kind: str, url: str, **_kwargs: Any):
        assert kind == 'api'
        captured.append(url)
        # Return any JSON-like object to satisfy return contract
        return {'ok': True}

    # Patch the client module's internal fetch used by _extract_with_retry
    monkeypatch.setattr(cmod, '_extract', fake_extract)

    # Simulate an ApiConfig.effective_base_url() of
    # 'https://api.example.com/v1'
    client = EndpointClient(
        base_url='https://api.example.com/v1',
        endpoints={'list': '/items'},
    )

    # No pagination provided -> single request path
    out = client.paginate('list', pagination=None)
    assert out == {'ok': True}

    # Verify composed URL includes the base path
    assert captured == ['https://api.example.com/v1/items']


def test_paginate_page_short_batch(monkeypatch):
    """Unit-test paginate: page/offset stops on short batch and aggregates."""

    # Fake extract to simulate two pages: 2 items then 1 item
    def fake_extract(kind: str, _url: str, **kwargs: Any):
        assert kind == 'api'
        params = kwargs.get('params') or {}
        page = int(params.get('page', 1))
        per_page = int(params.get('per_page', 2))
        assert per_page == 2
        if page == 1:
            return [{'id': 1}, {'id': 2}]
        if page == 2:
            return [{'id': 3}]
        return []

    # Patch the client module's internal fetch (used now that paginate is a
    # shim delegating to EndpointClient.paginate_url).
    monkeypatch.setattr(cmod, '_extract', fake_extract)

    url = 'https://example.test/api'
    params = {}
    headers = {}
    timeout = 5
    pagination = {
        'type': 'page',
        'page_param': 'page',
        'size_param': 'per_page',
        'start_page': 1,
        'page_size': 2,
        'records_path': None,
        'max_pages': None,
        'max_records': None,
    }
    client = EndpointClient(base_url='https://example.test', endpoints={})
    data = client.paginate_url(
        url, params, headers, timeout, pagination,
    )
    ids = [r['id'] for r in data]
    assert ids == [1, 2, 3]
