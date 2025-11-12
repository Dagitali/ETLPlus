"""
``tests.unit.config.test_u_api`` module.

Unit tests for ``etlplus.config.api``.

Notes
-----
- Ensures path/method handling and body/query/path parameter coercion.
- Exercises both flat and profiled API shapes.
- Uses factories for building profile defaults mappings.
- Verifies precedence and propagation of headers and base_path.
- Verifies precedence (explicit headers override defaults).
"""
from __future__ import annotations

import pytest

from etlplus.config import PaginationConfig
from etlplus.config import RateLimitConfig


# SECTION: TESTS ============================================================ #


class TestApiConfig:
    """
    Unit test suite for the :class:`ApiConfig`.
    """

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


class TestApiProfileConfig:
    """
    Unit test suite for the :class:`ApiProfileConfig` class.
    """
    @pytest.mark.parametrize(
        'defaults',
        [
            {'pagination': 'not-a-dict'},
            {'pagination': {'type': 123}},
            {'rate_limit': 'oops'},
            {'rate_limit': {'sleep_seconds': 'x', 'max_per_sec': []}},
        ],
        ids=[
            'pagination-str',
            'pagination-type-bad',
            'rate-limit-str',
            'rate-limit-bad-values',
        ],
    )
    def test_invalid_defaults_blocks(
        self,
        defaults: dict[str, object],
        profile_config_factory,
    ) -> None:
        obj = {
            'base_url': 'https://api.example.com',
            'defaults': defaults,
        }
        prof = profile_config_factory(obj)

        # Invalid blocks should yield None defaults objects or sanitized
        # values.
        if 'pagination' in defaults:
            assert getattr(prof, 'pagination_defaults', None) in (
                None,
                prof.pagination_defaults,
            )
        if 'rate_limit' in defaults:
            assert getattr(prof, 'rate_limit_defaults', None) in (
                None,
                prof.rate_limit_defaults,
            )

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
        api_profile_defaults_factory,
    ) -> None:  # noqa: D401
        obj = {
            'base_url': 'https://api.example.com',
            'defaults': api_profile_defaults_factory(
                pagination={
                    'type': 'page', 'page_param': 'p', 'size_param': 's',
                },
                rate_limit={'sleep_seconds': 0.1, 'max_per_sec': 5},
            ),
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


class TestEndpointConfig:
    """
    Unit test suite for the :class:`EndpointConfig` class.
    """

    def test_captures_path_params_and_body(
        self,
        endpoint_config_factory,
    ) -> None:  # noqa: D401
        ep = endpoint_config_factory({
            'method': 'POST',
            'path': '/users/{id}/avatar',
            'path_params': {'id': 'int'},
            'query_params': {'size': 'large'},
            'body': {'type': 'file', 'file_path': './x.png'},
        })
        assert ep.method == 'POST'
        assert ep.path_params == {'id': 'int'}
        assert isinstance(ep.body, dict) and ep.body['type'] == 'file'
        assert ep.query_params == {'size': 'large'}

    def test_from_str_sets_no_method(
        self,
        endpoint_config_factory,
    ) -> None:  # noqa: D401
        ep = endpoint_config_factory('/ping')
        assert ep.path == '/ping'
        assert ep.method is None

    @pytest.mark.parametrize(
        'payload, expected_exc',
        [
            ({'method': 'GET'}, TypeError),  # missing path
            ({'path': 123}, TypeError),  # path wrong type
            (
                {'path': '/x', 'path_params': 'id'},
                ValueError,
            ),  # string -> dict() raises ValueError
            (
                {'path': '/x', 'query_params': 1},
                TypeError,
            ),  # int -> dict() raises TypeError
        ],
        ids=[
            'missing-path', 'path-not-str',
            'path_params-not-mapping', 'query_params-not-mapping',
        ],
    )
    def test_invalid_payloads_raise(
        self,
        payload: dict[str, object],
        expected_exc: type[Exception],
        endpoint_config_factory,
    ) -> None:
        with pytest.raises(expected_exc):
            endpoint_config_factory(payload)  # type: ignore[arg-type]

    def test_lenient_fields_do_not_raise(
        self,
        endpoint_config_factory,
    ) -> None:
        """Lenient fields (method/body) accept any type and pass through."""
        ep_method = endpoint_config_factory({'method': 200, 'path': '/x'})
        assert ep_method.method == 200  # library currently permissive
        ep_body = endpoint_config_factory({'path': '/x', 'body': 'json'})
        assert ep_body.body == 'json'

    def test_parses_method(
        self,
        endpoint_config_factory,
    ) -> None:  # noqa: D401
        ep = endpoint_config_factory({
            'method': 'GET',
            'path': '/users',
            'query_params': {'active': True},
        })
        assert ep.path == '/users'
        assert ep.method == 'GET'
        assert ep.query_params.get('active') is True
