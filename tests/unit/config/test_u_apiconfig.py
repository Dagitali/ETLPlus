"""
``tests.unit.config.test_u_apiconfig`` module.

Unit tests for ``etlplus.config.api``.

Notes
-----
- Exercises both flat and profiled API shapes.
- Verifies precedence and propagation of headers and base_path.
"""
from __future__ import annotations

import pytest


# SECTION: TESTS ============================================================ #


class TestApiConfigProfiles:
    """
    Unit test suite for the :class:`ApiConfig` class with profiles.
    """

    def test_effective_base_url_and_build_endpoint_url(
        self,
        api_obj_factory,
        api_config_factory,
    ) -> None:
        obj = api_obj_factory(
            use_profiles=True,
            base_path='/v1',
            endpoints={'users': {'path': '/users'}},
        )
        cfg = api_config_factory(obj)

        # Effective base URL composes base_url + base_path.
        assert cfg.effective_base_url() == 'https://api.example.com/v1'
        url = cfg.build_endpoint_url(cfg.endpoints['users'])
        assert url == 'https://api.example.com/v1/users'

    def test_flat_shape_supported(
        self,
        api_obj_factory,
        api_config_factory,
    ) -> None:
        obj = api_obj_factory(
            use_profiles=False,
            base_path=None,
            headers={'X-Token': 'abc'},
            endpoints={'ping': '/ping'},
        )
        cfg = api_config_factory(obj)
        assert cfg.base_url == 'https://api.example.com'
        assert cfg.headers.get('X-Token') == 'abc'
        assert 'ping' in cfg.endpoints

    def test_parses_profiles_and_sets_defaults(
        self,
        api_config_factory,
    ) -> None:
        obj = {
            'profiles': {
                'default': {
                    'base_url': 'https://api.example.com/v1',
                    'headers': {'Accept': 'application/json'},
                },
                'prod': {
                    'base_url': 'https://api.example.com/v2',
                    'headers': {'Accept': 'application/json'},
                },
            },
            'endpoints': {'list': {'path': '/items'}},
        }
        cfg = api_config_factory(obj)

        # Default base_url/headers should be derived from the 'default'
        # profile.

        assert cfg.base_url == 'https://api.example.com/v1'
        assert cfg.headers.get('Accept') == 'application/json'

        # Profiles should be preserved.
        assert {'default', 'prod'} <= set(cfg.profiles.keys())

        # Endpoint should parse.
        assert 'list' in cfg.endpoints

    def test_profile_attr_with_default(self, api_config_factory) -> None:
        obj = {
            'profiles': {
                'default': {
                    'base_url': 'https://api.example.com',
                    'base_path': '/v1',
                    'defaults': {
                        'pagination': {'type': 'page'},
                        'rate_limit': {'sleep_seconds': 0.1},
                    },
                },
                'other': {'base_url': 'https://api.other'},
            },
            'endpoints': {},
        }
        cfg = api_config_factory(obj)

        # Effective getters rely on the internal helper; verify behavior.
        assert cfg.effective_base_path() == '/v1'
        assert cfg.effective_pagination_defaults() is not None

    def test_profile_attr_without_profiles_returns_none(
        self,
        api_config_factory,
    ) -> None:
        obj = {'base_url': 'https://api.example.com', 'endpoints': {}}
        cfg = api_config_factory(obj)
        assert cfg.effective_base_path() is None


class TestApiConfigDefaultsMapping:
    def test_profile_defaults_headers_and_fields(
        self,
        api_config_factory,
    ) -> None:
        obj = {
            'profiles': {
                'default': {
                    'base_url': 'https://api.example.com/v1',
                    'defaults': {
                        'headers': {
                            'Accept': 'application/json',
                            'X-From-Defaults': '1',
                        },
                    },
                    'headers': {
                        'Authorization': 'Bearer token',
                        'X-From-Defaults': '2',
                    },
                    'base_path': '/v1',
                    'auth': {'type': 'bearer', 'token': 'abc'},
                },
            },
            'headers': {'X-Top': 't'},
            'endpoints': {},
        }
        cfg = api_config_factory(obj)

        # Headers: defaults.headers < profile.headers < top-level.
        assert cfg.headers['Accept'] == 'application/json'
        assert cfg.headers['Authorization'] == 'Bearer token'

        # Profile.headers overrides defaults.
        assert cfg.headers['X-From-Defaults'] == '2'

        # Top-level overrides/augments.
        assert cfg.headers['X-Top'] == 't'

        # Profile extras captured.
        prof = cfg.profiles['default']
        assert prof.base_path == '/v1'
        assert prof.auth.get('type') == 'bearer'

    def test_profile_defaults_pagination_mapped(
        self,
        api_config_factory,
    ) -> None:
        obj = {
            'profiles': {
                'default': {
                    'base_url': 'https://api.example.com',
                    'defaults': {
                        'pagination': {
                            'type': 'page',
                            'params': {
                                'page': 'page',
                                'per_page': 'per_page',
                                'cursor': 'cursor',
                                'limit': 'limit',
                            },
                            'response': {
                                'items_path': 'data.items',
                                'next_cursor_path': 'meta.next_cursor',
                            },
                            'defaults': {'per_page': 25},
                            'max_pages': 10,
                        },
                    },
                },
            },
            'endpoints': {},
        }

        cfg = api_config_factory(obj)
        prof = cfg.profiles['default']
        pdef = getattr(prof, 'pagination_defaults', None)
        assert pdef is not None
        assert pdef.type == 'page'
        assert pdef.page_param == 'page'
        assert pdef.size_param == 'per_page'
        assert pdef.cursor_param == 'cursor'
        assert pdef.cursor_path == 'meta.next_cursor'
        assert pdef.records_path == 'data.items'
        assert pdef.page_size == 25
        assert pdef.max_pages == 10

    @pytest.mark.parametrize(
        'sleep,max_per',
        [
            (0.5, 2),
            (0.1, 10),
        ],
        ids=['basic', 'higher'],
    )
    def test_api_profile_defaults_rate_limit_mapped(
        self,
        api_config_factory,
        sleep,
        max_per,
    ) -> None:
        obj = {
            'profiles': {
                'default': {
                    'base_url': 'https://api.example.com',
                    'defaults': {
                        'rate_limit': {
                            'sleep_seconds': sleep,
                            'max_per_sec': max_per,
                        },
                    },
                },
            },
            'endpoints': {},
        }
        cfg = api_config_factory(obj)
        prof = cfg.profiles['default']
        rdef = getattr(prof, 'rate_limit_defaults', None)
        assert rdef is not None
        assert rdef.sleep_seconds == sleep
        assert rdef.max_per_sec == max_per
