"""
:mod:`tests.integration.test_i_run_profile_rate_limit_defaults` module.

Integration tests for profile-level rate limit defaults. Verifies propagation
of rate limit sleep configuration from API profile defaults into the runner and
ultimately the endpoint client, including overrides computed from
``max_per_sec``.

Notes
-----
- Parametrized across explicit sleep, computed sleep, conflict resolution,
    and absence of defaults.
- Uses ``run_patched`` to inject forced sleep seconds when computing from
    ``max_per_sec``.
"""

from __future__ import annotations

import pytest

from etlplus.api import RateLimitConfig

from .conftest import FakeEndpointClients
from .conftest import PipelineCfgFactory
from .conftest import RunPatched

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: CONSTANTS ======================================================== #


RATE_LIMIT_SLEEP_CASES = (
    pytest.param(
        RateLimitConfig(sleep_seconds=0.5, max_per_sec=None),
        None,
        0.5,
        id='sleep-seconds-direct',
    ),
    pytest.param(
        RateLimitConfig(sleep_seconds=None, max_per_sec=4),
        1.23,
        1.23,
        id='max-per-sec-computed',
    ),
    pytest.param(
        RateLimitConfig(sleep_seconds=0.2, max_per_sec=10),
        None,
        0.2,
        id='sleep-seconds-wins',
    ),
    pytest.param(None, None, 0.0, id='no-defaults-fallback-zero'),
)

# SECTION: TESTS ============================================================ #


class TestRunProfileRateLimitDefaults:
    """Integration tests for profile-level rate limit defaults."""

    @pytest.mark.parametrize(
        ('rate_cfg', 'forced_sleep', 'expected_sleep'),
        RATE_LIMIT_SLEEP_CASES,
    )
    def test_profile_rate_limit_sleep_propagation(
        self,
        pipeline_cfg_factory: PipelineCfgFactory,
        fake_endpoint_client: FakeEndpointClients,
        run_patched: RunPatched,
        rate_cfg: RateLimitConfig | None,
        forced_sleep: float | None,
        expected_sleep: float,
    ) -> None:
        """Test propagation of profile-level rate-limit sleep settings."""
        cfg = pipeline_cfg_factory(rate_limit_defaults=rate_cfg)

        fake_client, created = fake_endpoint_client
        result = run_patched(cfg, fake_client, sleep_seconds=forced_sleep)

        assert result.get('status') == 'ok'
        assert created, 'Expected client to be constructed'
        assert created[0].seen.get('sleep_seconds') == expected_sleep
