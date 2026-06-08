"""
:mod:`tests.unit.api.test_u_api_retry_manager` module.

Unit tests for :mod:`etlplus.api._retry_manager` helpers.
"""

from __future__ import annotations

from typing import cast

import pytest
import requests  # type: ignore[import]

from etlplus.api._errors import ApiAuthError
from etlplus.api._retry_manager import RetryManager
from etlplus.api._retry_manager import RetryPolicyDict
from etlplus.api._retry_manager import RetryStrategy

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestRetryStrategy:
    """Tests for :class:`RetryStrategy`."""

    @pytest.mark.parametrize(
        ('field_name', 'expected'),
        [
            pytest.param('max_attempts', 3, id='max-attempts'),
            pytest.param('backoff', pytest.approx(0.5), id='backoff'),
            pytest.param(
                'retry_on_codes',
                frozenset({429, 502, 503, 504}),
                id='retry-on-codes',
            ),
        ],
    )
    def test_defaults_when_policy_empty(
        self,
        field_name: str,
        expected: object,
    ) -> None:
        """Test that fallback to baked-in defaults when policy is empty."""
        strategy = RetryStrategy.from_policy({})
        assert getattr(strategy, field_name) == expected

    @pytest.mark.parametrize(
        ('field_name', 'expected'),
        [
            pytest.param('max_attempts', 5, id='max-attempts'),
            pytest.param('backoff', pytest.approx(0.1), id='backoff'),
            pytest.param('retry_on_codes', frozenset({429, 500}), id='retry-on'),
        ],
    )
    def test_policy_values_override_defaults(
        self,
        field_name: str,
        expected: object,
    ) -> None:
        """Test that provided policy values be normalized and honored."""
        strategy = RetryStrategy.from_policy(
            cast(
                RetryPolicyDict,
                {
                    'max_attempts': 5,
                    'backoff': 0.1,
                    'retry_on': [429, 500, 'oops'],  # type: ignore[list-item]
                },
            ),
        )
        assert getattr(strategy, field_name) == expected


class TestRetryManager:
    """Focused tests for :class:`RetryManager`."""

    def test_get_sleep_time_respects_cap(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that sleep time never exceeds the configured cap."""
        monkeypatch.setattr(
            'etlplus.api._retry_manager.random.uniform',
            lambda _a, b: b,
        )
        manager = RetryManager(
            policy={'max_attempts': 2, 'backoff': 10},
            cap=0.75,
        )
        assert manager.get_sleep_time(3) == pytest.approx(0.75)

    def test_should_retry_false_for_non_network_error_with_flag_enabled(
        self,
    ) -> None:
        """
        Test that *retry_network_errors* only retries timeout/connection
        exceptions.
        """
        manager = RetryManager(
            policy={'max_attempts': 2},
            retry_network_errors=True,
        )
        err = requests.HTTPError('bad')
        assert manager.should_retry(None, err) is False

    def test_should_retry_network_errors(self) -> None:
        """
        Test that network errors honors the ``retry_network_errors`` flag.
        """
        manager = RetryManager(
            policy={'max_attempts': 2},
            retry_network_errors=True,
        )
        err = requests.Timeout('boom')
        assert manager.should_retry(None, err) is True

    def test_should_retry_returns_false_when_not_retryable(self) -> None:
        """Test that non-retryable status/errors returns ``False``."""
        manager = RetryManager(
            policy={'max_attempts': 2, 'retry_on': [429]},
            retry_network_errors=False,
        )
        err = requests.HTTPError('bad')
        assert manager.should_retry(500, err) is False

    def test_raise_terminal_error_emits_auth_error_for_401(self) -> None:
        """Test that 401/403 terminal failures raises :class:`ApiAuthError`."""
        manager = RetryManager(policy={'max_attempts': 1})
        error = requests.HTTPError('auth')
        with pytest.raises(ApiAuthError):
            manager._raise_terminal_error(
                'https://example.test/auth',
                attempt=1,
                status=401,
                error=error,
            )
