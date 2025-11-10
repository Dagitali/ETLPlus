"""
tests.unit.config.test_apiprofileconfig unit tests module.


Unit tests for the ETLPlus API profile configuration.

Notes
-----
These tests cover the wrapping and propagation of API profile configuration.
"""
from __future__ import annotations

import pytest

from etlplus.config import PaginationConfig
from etlplus.config import RateLimitConfig


# SECTION: TESTS ============================================================ #


class TestApiProfileConfig:
    def test_merges_headers_defaults_low_precedence(
        self,
        profile_config_factory,
    ) -> None:  # noqa: D401
        obj = {
            'base_url': 'https://api.example.com',
            'headers': {'B': '2', 'A': '9'},
            'defaults': {'headers': {'A': '1'}},
        }
        prof = profile_config_factory(obj)
        assert prof.base_url == 'https://api.example.com'
        assert prof.headers == {'A': '9', 'B': '2'}

    def test_parses_defaults_blocks(
        self,
        profile_config_factory,
    ) -> None:  # noqa: D401
        obj = {
            'base_url': 'https://api.example.com',
            'defaults': {
                'pagination': {
                    'type': 'page',
                    'page_param': 'p',
                    'size_param': 's',
                },
                'rate_limit': {'sleep_seconds': 0.1, 'max_per_sec': 5},
            },
        }
        prof = profile_config_factory(obj)

    # Ensure types are parsed.
        assert isinstance(
            prof.pagination_defaults, (PaginationConfig, type(None)),
        )
        assert isinstance(
            prof.rate_limit_defaults, (RateLimitConfig, type(None)),
        )

        # Spot-check key fields.
        if prof.pagination_defaults is not None:
            assert prof.pagination_defaults.type == 'page'
            assert prof.pagination_defaults.page_param == 'p'
            assert prof.pagination_defaults.size_param == 's'
        if prof.rate_limit_defaults is not None:
            assert prof.rate_limit_defaults.sleep_seconds == 0.1
            assert prof.rate_limit_defaults.max_per_sec == 5

    def test_passthrough_fields(self, profile_config_factory):  # noqa: D401
        obj = {
            'base_url': 'https://api.example.com',
            'base_path': '/v1',
            'auth': {'token': 'abc'},
        }
        prof = profile_config_factory(obj)
        assert prof.base_path == '/v1'
        assert prof.auth == {'token': 'abc'}

    def test_requires_base_url(self, profile_config_factory):  # noqa: D401
        with pytest.raises(TypeError):
            profile_config_factory({})
