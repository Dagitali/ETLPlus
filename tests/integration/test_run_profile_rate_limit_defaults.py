"""
tests.integration.test_run_profile_rate_limit_defaults integration tests
module.

Integration tests for profile-level rate limit defaults.
"""
from __future__ import annotations

import pytest

from etlplus.config import RateLimitConfig


# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    'rate_cfg,forced_sleep,expected_sleep',
    [
        (RateLimitConfig(sleep_seconds=0.5, max_per_sec=None), None, 0.5),
        (RateLimitConfig(sleep_seconds=None, max_per_sec=4), 1.23, 1.23),
    ],
    ids=['sleep_seconds_direct', 'max_per_sec_compute'],
)
def test_profile_rate_limit_sleep_propagation(
    pipeline_cfg_factory,
    fake_endpoint_client,
    run_patched,
    rate_cfg: RateLimitConfig,
    forced_sleep: float | None,
    expected_sleep: float,
):
    cfg = pipeline_cfg_factory(rate_limit_defaults=rate_cfg)

    FakeClient, created = fake_endpoint_client
    result = run_patched(cfg, FakeClient, sleep_seconds=forced_sleep)

    # Sanity.
    assert result.get('status') == 'ok'
    assert created, 'Expected client to be constructed'

    # With only profile rate_limit defaults (sleep_seconds=0.5), the computed
    # sleep_seconds should reach the client.
    seen_sleep = created[0].seen.get('sleep_seconds')
    assert seen_sleep == expected_sleep
