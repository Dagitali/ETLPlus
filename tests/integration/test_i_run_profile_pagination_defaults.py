"""
:mod:`tests.integration.test_i_run_profile_pagination_defaults` module.

Integration tests for profile-level pagination defaults. Validates that
``run()`` inherits pagination defaults from the API profile when not overridden
and that job-level ``extract.options.pagination`` takes precedence over profile
defaults.

Notes
-----
- Uses in-memory pipeline config factory and a fake endpoint client.
- Asserts pagination mapping passed to the client matches expectations.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from etlplus.api import PaginationConfig
from etlplus.api import PaginationType
from etlplus.workflow import PipelineConfig
from tests.integration.conftest import FakeEndpointClientProtocol as Client

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.integration


# SECTION: TESTS ============================================================ #


class TestRunProfilePaginationDefaults:
    """Integration test suite for profile-level pagination defaults."""

    def test_job_level_pagination_overrides_profile_defaults(
        self,
        pipeline_cfg_factory: Callable[..., PipelineConfig],
        fake_endpoint_client: tuple[type[Client], list[Client]],
        run_patched: Callable[..., dict[str, Any]],
    ) -> None:
        """Test that job-level pagination options override profile defaults."""
        # Profile defaults exist, but job-level options will override.
        cfg = pipeline_cfg_factory(
            pagination_defaults=PaginationConfig(
                type=PaginationType.PAGE,
                page_param='page',
                size_param='per_page',
                start_page=5,
                page_size=50,
            ),
            extract_options={
                'pagination': {
                    'type': 'cursor',
                    'cursor_param': 'cursor',
                    'cursor_path': 'next',
                    'page_size': 25,
                },
            },
        )

        fake_client, created = fake_endpoint_client
        result = run_patched(cfg, fake_client)

        assert result.get('status') == 'ok'
        assert created, 'Expected client to be constructed'

        seen_pag = created[0].seen.get('pagination')
        assert isinstance(seen_pag, dict)
        # Verify cursor override took effect
        assert seen_pag.get('type') == 'cursor'
        assert seen_pag.get('cursor_param') == 'cursor'
        assert seen_pag.get('cursor_path') == 'next'
        assert seen_pag.get('page_size') == 25

    def test_profile_pagination_defaults_applied(
        self,
        pipeline_cfg_factory: Callable[..., PipelineConfig],
        fake_endpoint_client: tuple[type[Client], list[Client]],
        run_patched: Callable[..., dict[str, Any]],
    ) -> None:
        """Test that profile-level pagination defaults are applied."""
        cfg = pipeline_cfg_factory(
            pagination_defaults=PaginationConfig(
                type=PaginationType.PAGE,
                page_param='page',
                size_param='per_page',
                start_page=5,
                page_size=50,
            ),
        )

        fake_client, created = fake_endpoint_client
        result = run_patched(cfg, fake_client)

        # Sanity.
        assert result.get('status') == 'ok'
        assert created, 'Expected client to be constructed'

        # Assert the pagination dict came from the profile defaults.
        seen_pag = created[0].seen.get('pagination')
        assert isinstance(seen_pag, dict)
        assert seen_pag.get('type') == 'page'
        assert seen_pag.get('page_param') == 'page'
        assert seen_pag.get('size_param') == 'per_page'
        assert seen_pag.get('start_page') == 5
        assert seen_pag.get('page_size') == 50
