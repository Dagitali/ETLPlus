"""
Pagination unit tests
=====================

Tiny tests to lock in pagination invariants for page/offset and cursor modes.

We mock etlplus.extract.extract to simulate API responses and monkeypatch
time.sleep to avoid delays. We drive the CLI entrypoint to exercise the same
code paths as real usage without network calls.

Maintainer note:
- Pagination logic was modularized into ``etlplus.api.pagination.paginate``
  which calls an internal ``_extract`` on each page fetch.
- The CLI may still call ``etlplus.cli.extract`` on some code paths.
- To keep tests hermetic after modularization, we patch both targets:
  ``cli_mod.extract`` and ``pmod._extract``.
"""
from __future__ import annotations

from typing import Any

import etlplus.api.client as cmod
from etlplus.api.client import EndpointClient


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
