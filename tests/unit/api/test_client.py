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

import pytest

import etlplus.api.client as cmod
from etlplus.api import EndpointClient


def test_paginate_page_short_batch(monkeypatch):
    """Page pagination stops when last batch shorter than page_size."""

    def fake_extract(kind: str, _url: str, **kwargs: Any):
        assert kind == 'api'
        params = kwargs.get('params') or {}
        page = int(params.get('page', 1))
    # page_size=2 via config; emulate two pages then short batch
        if page == 1:
            return [{'id': 1}, {'id': 2}]
        if page == 2:
            return [{'id': 3}]
        return []

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    url = 'https://example.test/api'
    pagination = {
        'type': 'page',
        'page_param': 'page',
        'size_param': 'per_page',
        'start_page': 1,
        'page_size': 2,
        'records_path': None,
    }
    client = EndpointClient(base_url='https://example.test', endpoints={})
    data = client.paginate_url(url, None, None, 5, pagination)
    assert [r['id'] for r in data] == [1, 2, 3]


def test_paginate_page_max_records_cap(monkeypatch):
    """Respects max_records cap, truncating final batch as needed."""

    def fake_extract(kind: str, _url: str, **kwargs: Any):
        assert kind == 'api'
        params = kwargs.get('params') or {}
        page = int(params.get('page', 1))
        # Each page returns 3 records to force truncation
        return [{'p': page, 'i': i} for i in range(3)]

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    client = EndpointClient(base_url='https://example.test', endpoints={})
    pagination = {
        'type': 'page',
        'page_param': 'page',
        'size_param': 'per_page',
        'start_page': 1,
        'page_size': 3,
        'max_records': 5,  # should truncate second page (total would be 6)
    }
    data = client.paginate_url(
        'https://example.test/x', None, None, None, pagination,
    )
    assert len(data) == 5
    assert all('p' in r for r in data)


def test_paginate_page_size_normalization(monkeypatch):
    """Normalize page_size < 1 to 1 ensuring progress and termination."""

    def fake_extract(kind: str, _url: str, **kwargs: Any):
        assert kind == 'api'
        params = kwargs.get('params') or {}
        page = int(params.get('page', 1))
        # Return single record; page_size gets normalized to 1
        return [{'id': page}]

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    client = EndpointClient(base_url='https://example.test', endpoints={})
    pagination = {
        'type': 'page',
        'page_param': 'page',
        'size_param': 'per_page',
        'start_page': 1,
        'page_size': 0,  # invalid, should normalize
        'max_pages': 3,
    }
    data = client.paginate_url(
        'https://example.test/x', None, None, None, pagination,
    )
    assert [r['id'] for r in data] == [1, 2, 3]


@pytest.mark.parametrize(
    'base_url,base_path,endpoint,expected',
    [
        (
            'https://api.example.com',
            'v2',
            'items',
            'https://api.example.com/v2/items',
        ),
        (
            'https://api.example.com',
            '/v2',
            '/items',
            'https://api.example.com/v2/items',
        ),
        (
            'https://api.example.com/api',
            'v1',
            '/items',
            'https://api.example.com/api/v1/items',
        ),
        # Note: trailing slashes on base_url/base_path not normalized by client
    ],
)
def test_url_composition(monkeypatch, base_url, base_path, endpoint, expected):
    """URL composition should honor base_url path + base_path variants."""
    captured: list[str] = []

    def fake_extract(kind: str, url: str, **_kwargs: Any):
        assert kind == 'api'
        captured.append(url)
        return {'ok': True}

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    client = EndpointClient(
        base_url=base_url,
        endpoints={'list': endpoint},
        base_path=base_path,
    )
    out = client.paginate('list', pagination=None)
    assert out == {'ok': True}
    assert captured == [expected]
