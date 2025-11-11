"""
tests.integration.test_run_profile_rate_limit_defaults integration tests
module.

Integration tests for profile-level rate limit defaults
."""
from __future__ import annotations

from etlplus.config import RateLimitConfig


# SECTION: TESTS ============================================================ #


def test_profile_rate_limit_defaults_applied(
    pipeline_cfg_factory,
    fake_endpoint_client,
    run_patched,
):
    cfg = pipeline_cfg_factory(
        rate_limit_defaults=RateLimitConfig(
            sleep_seconds=0.5,
            max_per_sec=None,
        ),
    )

    FakeClient, created = fake_endpoint_client
    result = run_patched(cfg, FakeClient)

    # Sanity.
    assert result.get('status') == 'ok'
    assert created, 'Expected client to be constructed'

    # With only profile rate_limit defaults (sleep_seconds=0.5),
    # the computed sleep_seconds should reach the client.
    seen_sleep = created[0].seen.get('sleep_seconds')
    assert seen_sleep == 0.5


def test_profile_rate_limit_defaults_max_per_sec_applied(
    pipeline_cfg_factory,
    fake_endpoint_client,
    run_patched,
):
    cfg = pipeline_cfg_factory(
        rate_limit_defaults=RateLimitConfig(
            sleep_seconds=None,
            max_per_sec=4,
        ),
    )

    FakeClient, created = fake_endpoint_client
    result = run_patched(cfg, FakeClient, sleep_seconds=1.23)

    assert result.get('status') == 'ok'
    assert created, 'Expected client to be constructed'
    seen_sleep = created[0].seen.get('sleep_seconds')
    assert seen_sleep == 1.23
