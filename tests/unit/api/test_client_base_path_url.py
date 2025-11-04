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
