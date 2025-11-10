"""
tests.unit.api.test_client_pagination_offset_xfail unit tests module.

Document expected offset pagination behavior (pending implementation).

This test is marked xfail until 'offset' pagination type is supported.
"""
from __future__ import annotations

from typing import Any
from typing import cast

import pytest

import etlplus.api.client as cmod
from etlplus.api import EndpointClient
from etlplus.api.types import PaginationConfig


def test_offset_pagination_behaves_like_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_extract(kind: str, _url: str, **kwargs: dict[str, Any]):
        assert kind == 'api'
        calls.append(kwargs)
        params = kwargs.get('params') or {}
        off = int(params.get('offset', 0))
        limit = int(params.get('limit', 2))
        # return exactly `limit` items until offset reaches 4
        if off >= 4:
            return []
        return [{'i': i} for i in range(off, off + limit)]

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    client = EndpointClient(base_url='https://example.test', endpoints={})
    cfg = cast(
        PaginationConfig,
        {
            'type': 'offset',
            'page_param': 'offset',
            'size_param': 'limit',
            'start_page': 0,
            'page_size': 2,
            'max_records': 3,
        },
    )

    data = client.paginate_url(
        'https://example.test/api',
        None,
        None,
        None,
        cfg,
    )

    # Expected behavior: collects up to max_records using offset stepping
    assert [r['i'] for r in cast(list[dict[str, int]], data)] == [0, 1, 2]
