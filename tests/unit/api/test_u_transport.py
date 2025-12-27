"""
:mod:`tests.unit.api.test_u_transport` module.

Unit tests for :mod:`etlplus.api.transport`.

Notes
-----
- Validates mountability on a ``requests.Session``.
- Exercises integer and mapping forms of ``max_retries``.
- Ensures list/set inputs for retry fields are preserved.

Examples
--------
>>> pytest tests/unit/api/test_u_transport.py
"""

from __future__ import annotations

import pytest
import requests  # type: ignore[import]

from etlplus.api.transport import build_http_adapter
from etlplus.api.transport import build_session_with_adapters

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestBuildHttpAdapter:
    """
    Unit test suite for :func:`build_http_adapter`.

    Notes
    -----
    - Validates mountability and retry configuration handling.
    - Exercises integer, mapping, list, and set forms of max_retries fields.
    """

    def test_basic_mountable(self) -> None:
        """
        Test that build_http_adapter returns a mountable adapter and handles
        mapping max_retries.
        """
        cfg = {
            'pool_connections': 5,
            'pool_maxsize': 5,
            'pool_block': False,
            'max_retries': {'total': 3, 'backoff_factor': 0.1},
        }
        adapter = build_http_adapter(cfg)
        assert adapter is not None

        # Should be mountable on :class:`requests.Session`.
        s = requests.Session()
        s.mount('https://', adapter)

        # max_retries is either an int or a urllib3 Retry instance.
        mr = adapter.max_retries
        if isinstance(mr, int):
            assert mr == 3 or mr == 0
        else:
            # Retry object exposes total when urllib3 is available.
            total = getattr(mr, 'total', None)
            assert total in (0, 3)

    def test_build_http_adapter_invalid_config(self):
        """
        Test that invalid config does not raise and returns a usable adapter.
        """
        cfg = {
            'pool_connections': 'not-an-int',
            'max_retries': {'total': 'bad'},
        }
        adapter = build_http_adapter(cfg)
        assert isinstance(adapter, requests.adapters.HTTPAdapter)

    def test_build_http_adapter_missing_keys(self):
        """Test that missing keys are handled gracefully."""
        cfg = {}
        adapter = build_http_adapter(cfg)
        assert isinstance(adapter, requests.adapters.HTTPAdapter)

    def test_build_http_adapter_retry_dict_edge(self):
        """Test retry dict with unknown keys is ignored."""
        cfg = {
            'max_retries': {'total': 2, 'unknown_key': 123},
        }
        adapter = build_http_adapter(cfg)
        mr = adapter.max_retries
        if isinstance(mr, int):
            assert mr in (0, 2)
        else:
            assert getattr(mr, 'total', None) in (0, 2)

    def test_build_session_with_adapters_invalid(self):
        """
        Test that invalid adapter configs are skipped but session is usable.
        """
        # pylint: disable=broad-exception-caught
        adapters_cfg = [
            {'prefix': 'https://', 'pool_connections': 'bad'},
            {'prefix': 'http://', 'max_retries': {'total': 'bad'}},
        ]
        session = requests.Session()
        # Should not raise
        try:
            session = build_session_with_adapters(adapters_cfg)
        except Exception:
            pytest.fail('build_session_with_adapters should not raise')
        assert isinstance(session, requests.Session)

    def test_integer_retries_fallback(self) -> None:
        """Test handling integer max_retries fallback."""
        cfg = {
            'pool_connections': 2,
            'pool_maxsize': 2,
            'pool_block': True,
            'max_retries': 5,
        }
        adapter = build_http_adapter(cfg)
        assert adapter is not None

        # When an integer is provided, requests converts it into a
        # :class:`Retry` instance in newer versions; support either int or
        # ``Retry(total=5)`` depending on implementation details.
        mr = adapter.max_retries
        if isinstance(mr, int):
            assert mr == 5
        else:
            assert getattr(mr, 'total', None) == 5

    def test_retry_coercion_lists(self) -> None:
        """
        Test handling list inputs for allowed_methods and status_forcelist.
        """
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
            # Environment without urllib3 Retry available; nothing to assert
            # here about mapping details.
            assert mr in (0, 2)
            return

        am = getattr(mr, 'allowed_methods', None)
        sf = getattr(mr, 'status_forcelist', None)

        # allowed_methods should include provided methods (normalized upper).
        assert am is not None
        assert {m.upper() for m in am} == {'GET', 'POST'}

        # status_forcelist should include provided statuses.
        assert sf is not None
        assert set(sf) == {429, 500}

    def test_retry_coercion_sets(self) -> None:
        """
        Test handling set and frozenset inputs for allowed_methods and
        status_forcelist.
        """
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
