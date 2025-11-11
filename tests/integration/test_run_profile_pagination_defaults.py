"""
tests.integration.test_run_profile_pagination_defaults integration tests
module.

Integration tests for profile-level pagination defaults.

Verifies `run()` inherits pagination defaults from API profile when not
overridden.
"""
from __future__ import annotations

from etlplus.config import PaginationConfig


# SECTION: TESTS ============================================================ #


def test_profile_pagination_defaults_applied(
    pipeline_cfg_factory,
    fake_endpoint_client,
    run_patched,
):
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
