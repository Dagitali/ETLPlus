"""
:mod:`tests.unit.api.test_u_retry_manager` module.

Unit tests for :mod:`etlplus.api.retry_manager` helpers.
"""

from __future__ import annotations

from typing import cast

import pytest
import requests  # type: ignore[import]

from etlplus.api.retry_manager import RetryManager
from etlplus.api.retry_manager import RetryPolicy
from etlplus.api.retry_manager import RetryStrategy

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


class TestRetryStrategy:
    """Tests for :class:`RetryStrategy`."""

    def test_defaults_when_policy_empty(self) -> None:
        """Fallback to baked-in defaults when policy is empty."""
        strategy = RetryStrategy.from_policy({})
        assert strategy.max_attempts == 3
        assert strategy.backoff == pytest.approx(0.5)
        assert strategy.retry_on_codes == frozenset({429, 502, 503, 504})

    def test_policy_values_override_defaults(self) -> None:
        """Provided policy values should be normalized and honored."""
        strategy = RetryStrategy.from_policy(
            cast(
                RetryPolicy,
                {
                    'max_attempts': 5,
                    'backoff': 0.1,
                    'retry_on': [429, 500, 'oops'],  # type: ignore[list-item]
                },
            ),
        )
        assert strategy.max_attempts == 5
        assert strategy.backoff == pytest.approx(0.1)
        assert strategy.retry_on_codes == frozenset({429, 500})


class TestRetryManager:
    """Focused tests for :class:`RetryManager`."""

    def test_get_sleep_time_respects_cap(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """sleep time should never exceed the configured cap."""
        monkeypatch.setattr(
            'etlplus.api.retry_manager.random.uniform',
            lambda _a, b: b,
        )
        manager = RetryManager(
            policy={'max_attempts': 2, 'backoff': 10},
            cap=0.75,
        )
        assert manager.get_sleep_time(3) == pytest.approx(0.75)

    def test_should_retry_network_errors(self) -> None:
        """Network errors should honor the ``retry_network_errors`` flag."""
        manager = RetryManager(
            policy={'max_attempts': 2},
            retry_network_errors=True,
        )
        err = requests.Timeout('boom')
        assert manager.should_retry(None, err) is True
