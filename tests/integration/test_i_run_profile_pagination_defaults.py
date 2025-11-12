"""
``tests.integration.test_i_run_profile_pagination_defaults`` module.

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

from etlplus.config import PaginationConfig


# SECTION: TESTS ============================================================ #


class TestRunProfilePaginationDefaults:

    def test_job_level_pagination_overrides_profile_defaults(
        self,
        pipeline_cfg_factory,
        fake_endpoint_client,
        run_patched,
    ) -> None:
        # Profile defaults exist, but job-level options will override.
        cfg = pipeline_cfg_factory(
            pagination_defaults=PaginationConfig(
                type='page',
                page_param='page',
                size_param='per_page',
                start_page=5,
                page_size=50,
            ),
        )
        job = cfg.jobs[0]
        job.extract.options = {
            'pagination': {
                'type': 'cursor',
                'cursor_param': 'cursor',
                'cursor_path': 'next',
                'page_size': 25,
            },
        }

        FakeClient, created = fake_endpoint_client
        result = run_patched(cfg, FakeClient)

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
        pipeline_cfg_factory,
        fake_endpoint_client,
        run_patched,
    ) -> None:
        cfg = pipeline_cfg_factory(
            pagination_defaults=PaginationConfig(
                type='page',
                page_param='page',
                size_param='per_page',
                start_page=5,
                page_size=50,
            ),
        )

        FakeClient, created = fake_endpoint_client
        result = run_patched(cfg, FakeClient)

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
