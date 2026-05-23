"""
:mod:`tests.integration.test_i_run_profile_pagination_defaults` module.

Integration tests for profile-level pagination defaults. Validates that
:func:`run` inherits pagination defaults from the API profile when not
overridden and that job-level ``extract.options.pagination`` takes precedence
over profile :class:`PaginationConfig` defaults.

Notes
-----
- Uses in-memory pipeline config factory and a fake endpoint client.
- Asserts pagination mapping passed to the client matches expectations.
"""

from __future__ import annotations

import pytest

from etlplus.api import PaginationConfig
from etlplus.api import PaginationType

from .conftest import FakeEndpointClients
from .conftest import PipelineCfgFactory
from .conftest import RunPatched

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: CONSTANTS ======================================================== #


PROFILE_PAGINATION_DEFAULTS = PaginationConfig(
    type=PaginationType.PAGE,
    page_param='page',
    size_param='per_page',
    start_page=5,
    page_size=50,
)
PROFILE_PAGINATION_CASES = (
    pytest.param(
        PROFILE_PAGINATION_DEFAULTS,
        None,
        {
            'type': 'page',
            'page_param': 'page',
            'size_param': 'per_page',
            'start_page': 5,
            'page_size': 50,
        },
        id='profile-defaults-applied',
    ),
    pytest.param(
        PROFILE_PAGINATION_DEFAULTS,
        {
            'pagination': {
                'type': 'cursor',
                'cursor_param': 'cursor',
                'cursor_path': 'next',
                'page_size': 25,
            },
        },
        {
            'type': 'cursor',
            'cursor_param': 'cursor',
            'cursor_path': 'next',
            'page_size': 25,
        },
        id='job-options-override-profile-defaults',
    ),
)

# SECTION: TESTS ============================================================ #


class TestRunProfilePaginationDefaults:
    """Integration tests for profile-level pagination defaults."""

    @pytest.mark.parametrize(
        ('pagination_defaults', 'extract_options', 'expected_pagination'),
        PROFILE_PAGINATION_CASES,
    )
    def test_profile_pagination_resolution(
        self,
        pipeline_cfg_factory: PipelineCfgFactory,
        fake_endpoint_client: FakeEndpointClients,
        run_patched: RunPatched,
        pagination_defaults: PaginationConfig,
        extract_options: dict[str, object] | None,
        expected_pagination: dict[str, object],
    ) -> None:
        """Test profile pagination defaults and job-level overrides."""
        cfg = pipeline_cfg_factory(
            pagination_defaults=pagination_defaults,
            extract_options=extract_options,
        )

        fake_client, created = fake_endpoint_client
        result = run_patched(cfg, fake_client)

        assert result.get('status') == 'ok'
        assert created, 'Expected client to be constructed'

        seen_pag = created[0].seen.get('pagination')
        assert isinstance(seen_pag, dict)
        assert {
            key: seen_pag.get(key) for key in expected_pagination
        } == expected_pagination
