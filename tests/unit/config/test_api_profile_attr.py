"""
ETLPlus API Profile Attribute Tests
===================================

Unit tests for the ETLPlus API profile attributes.

Notes
-----
These tests cover the wrapping and propagation of API profile attributes.
"""
from __future__ import annotations

from etlplus.config.api import ApiConfig


def test_profile_attr_with_default_profile():
    cfg = ApiConfig.from_obj(
        {
            'profiles': {
                'default': {
                    'base_url': 'https://api.example.com',
                    'base_path': '/v1',
                    'defaults': {
                        'pagination': {'type': 'page'},
                        'rate_limit': {'sleep_seconds': 0.1},
                    },
                },
                'other': {
                    'base_url': 'https://api.other',
                },
            },
            'endpoints': {},
        },
    )

    # Effective getters rely on the internal helper; verify behavior
    assert cfg.effective_base_path() == '/v1'
    assert cfg.effective_pagination_defaults() is not None


def test_profile_attr_without_profiles_returns_none():
    cfg = ApiConfig.from_obj(
        {'base_url': 'https://api.example.com', 'endpoints': {}},
    )
    assert cfg.effective_base_path() is None
