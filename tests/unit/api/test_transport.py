"""
tests.unit.api.transport unit tests module.

Smoke tests for etlplus.api.transport.
"""
from __future__ import annotations

import requests  # type: ignore[import]

from etlplus.api.transport import build_http_adapter


# SECTION: TESTS ============================================================ #


def test_build_http_adapter_basic_mountable() -> None:
    cfg = {
        'pool_connections': 5,
        'pool_maxsize': 5,
        'pool_block': False,
        'max_retries': {'total': 3, 'backoff_factor': 0.1},
    }
    adapter = build_http_adapter(cfg)
    assert adapter is not None

    # Should be mountable on a requests Session
    s = requests.Session()
    s.mount('https://', adapter)

    # max_retries is either an int or a urllib3 Retry instance
    mr = adapter.max_retries
    if isinstance(mr, int):
        assert mr == 3 or mr == 0
    else:
        # Retry object exposes total when urllib3 is available
        total = getattr(mr, 'total', None)
        assert total in (0, 3)


def test_build_http_adapter_integer_retries_fallback() -> None:
    cfg = {
        'pool_connections': 2,
        'pool_maxsize': 2,
        'pool_block': True,
        'max_retries': 5,  # integer fallback path
    }
    adapter = build_http_adapter(cfg)
    assert adapter is not None
    # When an integer is provided, requests converts it into a Retry instance
    # in newer versions; support either int or Retry(total=5) depending on
    # implementation details.
    mr = adapter.max_retries
    if isinstance(mr, int):
        assert mr == 5
    else:
        assert getattr(mr, 'total', None) == 5


def test_build_http_adapter_retry_coercion_lists() -> None:
    # Provide lists for allowed_methods & status_forcelist; ensure they
    # map onto the Retry object irrespective of concrete container type.
    cfg = {
        'pool_connections': 2,
        'pool_maxsize': 2,
        'pool_block': False,
        'max_retries': {
            'total': 2,
            'backoff_factor': 0.1,
            'allowed_methods': ['get', 'POST'],
            'status_forcelist': [429, 500],
        },
    }
    adapter = build_http_adapter(cfg)
    mr = adapter.max_retries
    if isinstance(mr, int):
        # Environment without urllib3 Retry available; nothing to assert here
        # about mapping details.
        assert mr in (0, 2)
        return

    am = getattr(mr, 'allowed_methods', None)
    sf = getattr(mr, 'status_forcelist', None)

    # allowed_methods should include provided methods (normalized upper)
    assert am is not None
    assert {m.upper() for m in am} == {'GET', 'POST'}

    # status_forcelist should include provided statuses
    assert sf is not None
    assert set(sf) == {429, 500}


def test_build_http_adapter_retry_coercion_sets() -> None:
    # Provide sets to exercise set and frozenset handling in mapping.
    cfg = {
        'pool_connections': 2,
        'pool_maxsize': 2,
        'pool_block': False,
        'max_retries': {
            'total': 1,
            'allowed_methods': {'get', 'post', 'PUT'},
            'status_forcelist': {502, 503},
        },
    }
    adapter = build_http_adapter(cfg)
    mr = adapter.max_retries
    if isinstance(mr, int):
        assert mr in (0, 1)
        return

    am = getattr(mr, 'allowed_methods', None)
    sf = getattr(mr, 'status_forcelist', None)

    assert am is not None
    assert {m.upper() for m in am} == {'GET', 'POST', 'PUT'}
    assert sf is not None
    assert set(sf) == {502, 503}
