"""
:mod:`tests.integration.api.test_i_api_endpoint_client` module.

Integration tests for :mod:`etlplus.api.endpoint_client` glue wiring.
"""

from __future__ import annotations

import types
from typing import Any

import pytest
import requests  # type: ignore[import]

import etlplus.api._request_manager as rm_module
import etlplus.api.rate_limiting.rate_limiter as rl_module
from etlplus.api import EndpointClient
from etlplus.api import PagePaginationConfigDict
from etlplus.api import PaginationType

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


# pylint: disable=unused-argument


# SECTION: TESTS ============================================================ #


def test_local_fake_http_flow_wires_endpoint_client_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test that :class:`EndpointClient` composes URL, paginates, retries
    transient errors, and enforces rate-limit pacing between pages.
    """
    request_calls: list[dict[str, Any]] = []
    attempts_by_page: dict[int, int] = {}
    enforced_sleeps: list[float] = []

    def _capture_rate_limit(self: rl_module.RateLimiter) -> None:
        enforced_sleeps.append(self.sleep_seconds)

    monkeypatch.setattr(rl_module.RateLimiter, 'enforce', _capture_rate_limit)

    def _fake_request_once(
        self: rm_module.RequestManager,  # noqa: ARG001
        method: str,
        url: str,
        *,
        session: Any,  # noqa: ARG001
        timeout: Any,  # noqa: ARG001
        request_callable: Any = None,  # noqa: ARG001
        **kwargs: Any,
    ) -> dict[str, Any]:
        params = dict(kwargs.get('params') or {})
        page = int(params.get('page', 1))
        attempts_by_page[page] = attempts_by_page.get(page, 0) + 1
        request_calls.append(
            {'method': method, 'url': url, 'params': params},
        )

        # Fail the first page once to force retry wiring.
        if page == 1 and attempts_by_page[page] == 1:
            err = requests.HTTPError('transient')
            err.response = types.SimpleNamespace(status_code=503, text='retry')
            raise err

        if page == 1:
            return {'items': [{'id': 1}, {'id': 2}]}
        return {'items': []}

    monkeypatch.setattr(
        rm_module.RequestManager,
        'request_once',
        _fake_request_once,
    )

    client = EndpointClient(
        base_url='https://example.test/api',
        endpoints={'items': '/v1/{tenant}/items'},
        retry={'max_attempts': 2, 'backoff': 0.0, 'retry_on': [503]},
        rate_limit={'max_per_sec': 2},
    )

    pagination: PagePaginationConfigDict = {
        'type': PaginationType.PAGE,
        'page_param': 'page',
        'size_param': 'per_page',
        'page_size': 2,
        'records_path': 'items',
    }

    rows = client.paginate(
        'items',
        path_parameters={'tenant': 'acme corp'},
        query_parameters={'q': 'x y'},
        pagination=pagination,
        rate_limit_overrides={'max_per_sec': 8},
    )

    assert rows == [{'id': 1}, {'id': 2}]
    assert attempts_by_page[1] == 2
    assert attempts_by_page[2] == 1
    assert request_calls[0]['url'] == (
        'https://example.test/api/v1/acme%20corp/items?q=x+y'
    )
    assert request_calls[0]['method'] == 'GET'
    assert request_calls[0]['params'] == {'page': 1, 'per_page': 2}
    assert enforced_sleeps == [pytest.approx(0.125)]
