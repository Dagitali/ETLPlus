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
