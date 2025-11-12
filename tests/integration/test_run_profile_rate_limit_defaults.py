"""
``tests.integration.test_run_profile_rate_limit_defaults`` module.

Integration tests for profile-level rate limit defaults.

Verifies propagation of rate limit sleep configuration from API profile
defaults into the runner and ultimately the endpoint client, including
overrides computed from ``max_per_sec``.

Notes
-----
- Parametrized across explicit sleep, computed sleep, conflict resolution,
    and absence of defaults.
- Uses ``run_patched`` to inject forced sleep seconds when computing from
    ``max_per_sec``.
"""
from __future__ import annotations

import pytest

from etlplus.config import RateLimitConfig


# SECTION: TESTS ============================================================ #


class TestRunProfileRateLimitDefaults:
    @pytest.mark.parametrize(
        'rate_cfg,forced_sleep,expected_sleep',
        [
            # Explicit sleep_seconds should propagate directly
            (RateLimitConfig(sleep_seconds=0.5, max_per_sec=None), None, 0.5),
            # max_per_sec path: runner computes (we force via run_patched)
            (RateLimitConfig(sleep_seconds=None, max_per_sec=4), 1.23, 1.23),
            # Both provided: explicit sleep_seconds wins
            (RateLimitConfig(sleep_seconds=0.2, max_per_sec=10), None, 0.2),
            # No defaults: expect fallback to 0.0
            (None, None, 0.0),
        ],
        ids=[
            'sleep_seconds_direct',
            'max_per_sec_compute',
            'both_values_sleep_wins',
            'no_defaults_fallback_zero',
        ],
    )
    def test_profile_rate_limit_sleep_propagation(
        self,
        pipeline_cfg_factory,
        fake_endpoint_client,
        run_patched,
        rate_cfg: RateLimitConfig | None,
        forced_sleep: float | None,
        expected_sleep: float,
    ) -> None:
        cfg = pipeline_cfg_factory(rate_limit_defaults=rate_cfg)

        FakeClient, created = fake_endpoint_client
        result = run_patched(cfg, FakeClient, sleep_seconds=forced_sleep)

        # Sanity.
        assert result.get('status') == 'ok'
        assert created, 'Expected client to be constructed'

        # With only profile rate_limit defaults (sleep_seconds=0.5), the
        # computed sleep_seconds should reach the client.
        seen_sleep = created[0].seen.get('sleep_seconds')
        assert seen_sleep == expected_sleep
