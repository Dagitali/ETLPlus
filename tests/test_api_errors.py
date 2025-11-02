"""
ETLPlus API Error Tests
=======================

Unit tests for the ETLPlus API error handling.

Notes
-----
These tests cover the wrapping and propagation of API errors.
"""
from typing import Any

import pytest
import requests  # type: ignore

from etlplus.api.client import EndpointClient


class DummyResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


def make_http_error(status: int) -> requests.HTTPError:
    err = requests.HTTPError(f"HTTP {status}")
    # Attach a response-like object that exposes status_code
    resp = requests.Response()
    resp.status_code = status
    # type: ignore[attr-defined]
    err.response = resp
    return err


def test_auth_error_wrapping_on_single_attempt(monkeypatch: Any) -> None:
    client = EndpointClient(
        base_url='https://api.example.com/v1',
        endpoints={'x': '/x'},
    )

    def boom(_stype: str, url: str, **kw: Any):  # noqa: ARG001
        raise make_http_error(401)

    # Patch the module-level _extract symbol imported in client
    monkeypatch.setattr('etlplus.api.client._extract', boom)

    with pytest.raises(Exception) as ei:
        client.paginate_url(
            'https://api.example.com/v1/x', None, None, None, None,
        )

    err = ei.value
    from etlplus.api.errors import ApiAuthError

    assert isinstance(err, ApiAuthError)
    assert err.status == 401
    assert err.attempts == 1
    assert err.retried is False
    assert err.retry_policy is None


def test_request_error_after_retries_exhausted(monkeypatch: Any) -> None:
    # Retry twice, both 503
    client = EndpointClient(
        base_url='https://api.example.com/v1',
        endpoints={'x': '/x'},
        retry={'max_attempts': 2, 'backoff': 0.0, 'retry_on': [503]},
    )

    attempts = {'n': 0}

    def boom(_stype: str, _url: str, **_kw: Any):  # noqa: ARG001
        attempts['n'] += 1
        raise make_http_error(503)

    monkeypatch.setattr('etlplus.api.client._extract', boom)

    with pytest.raises(Exception) as ei:
        client.paginate_url(
            'https://api.example.com/v1/x', None, None, None, None,
        )

    from etlplus.api.errors import ApiRequestError

    err = ei.value
    assert isinstance(err, ApiRequestError)
    assert err.status == 503
    assert err.attempts == 2  # exhausted
    assert err.retried is True


def test_pagination_error_includes_page_number(monkeypatch: Any) -> None:
    client = EndpointClient(
        base_url='https://api.example.com/v1',
        endpoints={'list': '/items'},
    )

    page_size = 2

    def extractor(_stype: str, _url: str, **kw: Any):  # noqa: ARG001
        params = kw.get('params') or {}
        page = int(params.get('page', 1))
        size = int(params.get('per_page', page_size))
        if page == 4:
            raise make_http_error(500)
        # Return exactly `size` records to force continue until failure
        return {'items': [{'i': i} for i in range(size)]}

    monkeypatch.setattr('etlplus.api.client._extract', extractor)

    pagination: dict[str, Any] = {
        'type': 'page',
        'page_param': 'page',
        'size_param': 'per_page',
        'start_page': 3,
        'page_size': page_size,
        'records_path': 'items',
    }

    with pytest.raises(Exception) as ei:
        client.paginate(
            'list', pagination=pagination,  # type: ignore[arg-type]
        )

    from etlplus.api.errors import PaginationError

    err = ei.value
    assert isinstance(err, PaginationError)
    assert err.page == 4
    assert err.status == 500
