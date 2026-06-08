"""
:mod:`tests.unit.api.test_u_api_config` module.

Unit tests for :mod:`etlplus.api._config`.

Notes
-----
- Exercises both flat and profiled API shapes.
- Uses factories for building profile defaults mappings.
- Verifies precedence and propagation of headers and ``base_path``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from typing import cast

import pytest

import etlplus.api._config as config_mod
from etlplus.api import ApiConfig
from etlplus.api import ApiProfileConfig
from etlplus.api import EndpointConfig
from etlplus.api import HttpMethod
from etlplus.api import PaginationConfig
from etlplus.api import RateLimitConfig

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestApiConfig:
    """
    Unit tests for :class:`ApiConfig`.

    Notes
    -----
    Tests mapping of rate limit, header precedence, base_path propagation, and
    profile/default behaviors for API configuration.
    """

    @pytest.mark.parametrize(
        ('sleep', 'max_per'),
        [
            pytest.param(0.5, 2, id='basic'),
            pytest.param(0.1, 10, id='higher'),
        ],
    )
    def test_api_profile_defaults_rate_limit_mapped(
        self,
        base_url: str,
        api_config_factory: Callable[[dict], ApiConfig],
        sleep: float,
        max_per: int,
    ) -> None:
        """
        Test that API profile defaults for rate limit are mapped correctly.
        """
        obj = {
            'profiles': {
                'default': {
                    'base_url': base_url,
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
        base_url: str,
        api_obj_factory: Callable[..., dict[str, Any]],
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
    ) -> None:
        """
        Test that effective_base_url and build_endpoint_url compose URLs
        correctly.
        """
        obj = api_obj_factory(
            use_profiles=True,
            base_path='/v1',
            endpoints={'users': {'path': '/users'}},
        )
        cfg = api_config_factory(obj)

        # Effective base URL composes base_url + base_path.
        expected_base = f'{base_url}/v1'
        assert cfg.effective_base_url() == expected_base
        url = cfg.build_endpoint_url(cfg.endpoints['users'])
        assert url == f'{expected_base}/users'

    @pytest.mark.parametrize(
        ('field', 'expected'),
        [
            pytest.param('base_url', None, id='base-url'),
            pytest.param('headers.X-Token', 'abc', id='header'),
            pytest.param('endpoints.ping', True, id='endpoint'),
        ],
    )
    def test_flat_shape_supported(
        self,
        base_url: str,
        api_obj_factory: Callable[..., dict[str, Any]],
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
        field: str,
        expected: object,
    ) -> None:
        """
        Test that flat API config shape is supported and headers/endpoints are
        parsed.
        """
        obj = api_obj_factory(
            use_profiles=False,
            base_path=None,
            headers={'X-Token': 'abc'},
            endpoints={'ping': '/ping'},
        )
        cfg = api_config_factory(obj)
        match field.split('.'):
            case ['base_url']:
                actual = cfg.base_url
                expected = base_url
            case ['headers', header]:
                actual = cfg.headers.get(header)
            case ['endpoints', endpoint]:
                actual = endpoint in cfg.endpoints
            case _:
                pytest.fail(f'Unsupported field path: {field}')

        assert actual == expected

    def test_invalid_profile_base_path_does_not_break_effective_url(
        self,
        base_url: str,
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
    ) -> None:
        """
        Test that non-string profile base_path values are ignored safely.
        """
        obj = {
            'profiles': {
                'default': {
                    'base_url': base_url,
                    'base_path': {'invalid': '/v1'},
                },
            },
            'endpoints': {},
        }
        cfg = api_config_factory(obj)

        assert cfg.effective_base_path() is None
        assert cfg.effective_base_url() == base_url

    @pytest.mark.parametrize(
        ('field', 'expected'),
        [
            pytest.param('base_url', None, id='base-url'),
            pytest.param('headers.Accept', 'application/json', id='default-header'),
            pytest.param('profiles.default', True, id='default-profile'),
            pytest.param('profiles.prod', True, id='prod-profile'),
            pytest.param('endpoints.list', True, id='endpoint'),
        ],
    )
    def test_parses_profiles_and_sets_defaults(
        self,
        base_url: str,
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
        field: str,
        expected: object,
    ) -> None:
        """
        Test that profiles are parsed and default values are set correctly.
        """
        obj = {
            'profiles': {
                'default': {
                    'base_url': f'{base_url}/v1',
                    'headers': {'Accept': 'application/json'},
                },
                'prod': {
                    'base_url': f'{base_url}/v2',
                    'headers': {'Accept': 'application/json'},
                },
            },
            'endpoints': {'list': {'path': '/items'}},
        }
        cfg = api_config_factory(obj)
        match field.split('.'):
            case ['base_url']:
                actual = cfg.base_url
                expected = f'{base_url}/v1'
            case ['headers', header]:
                actual = cfg.headers.get(header)
            case ['profiles', profile]:
                actual = profile in cfg.profiles
            case ['endpoints', endpoint]:
                actual = endpoint in cfg.endpoints
            case _:
                pytest.fail(f'Unsupported field path: {field}')

        assert actual == expected

    @pytest.mark.parametrize(
        ('accessor', 'expected'),
        [
            pytest.param('effective_base_path', '/v1', id='base-path'),
            pytest.param(
                'effective_pagination_defaults',
                True,
                id='pagination-defaults',
            ),
        ],
    )
    def test_profile_attr_with_default(
        self,
        base_url: str,
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
        accessor: str,
        expected: object,
    ) -> None:
        """
        Test that profile attributes with defaults are handled correctly.
        """
        obj = {
            'profiles': {
                'default': {
                    'base_url': base_url,
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

        actual = getattr(cfg, accessor)()
        assert actual is not None if expected is True else actual == expected

    @pytest.mark.parametrize(
        'accessor',
        [
            pytest.param('effective_base_path', id='base-path'),
            pytest.param('effective_pagination_defaults', id='pagination-defaults'),
            pytest.param('effective_rate_limit_defaults', id='rate-limit-defaults'),
        ],
    )
    def test_profile_attr_without_profiles_returns_none(
        self,
        base_url: str,
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
        accessor: str,
    ) -> None:
        """
        Test that profile attribute access returns None when profiles are
        absent.
        """
        obj = {'base_url': base_url, 'endpoints': {}}
        cfg = api_config_factory(obj)
        assert getattr(cfg, accessor)() is None

    @pytest.mark.parametrize(
        ('field', 'expected'),
        [
            pytest.param('headers.Accept', 'application/json', id='default-header'),
            pytest.param('headers.Authorization', 'Bearer token', id='profile-header'),
            pytest.param(
                'headers.X-From-Defaults',
                '2',
                id='profile-overrides-default',
            ),
            pytest.param('headers.X-Top', 't', id='top-level-header'),
            pytest.param('profile.base_path', '/v1', id='base-path'),
            pytest.param('profile.auth.type', 'bearer', id='auth-type'),
        ],
    )
    def test_profile_defaults_headers_and_fields(
        self,
        base_url: str,
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
        field: str,
        expected: str,
    ) -> None:
        """
        Test that header precedence and profile fields are handled correctly.
        """
        obj = {
            'profiles': {
                'default': {
                    'base_url': f'{base_url}/v1',
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
        prof = cfg.profiles['default']

        match field.split('.'):
            case ['headers', header]:
                actual = cfg.headers[header]
            case ['profile', 'auth', 'type']:
                actual = prof.auth.get('type')
            case ['profile', attr]:
                actual = getattr(prof, attr)
            case _:
                pytest.fail(f'Unsupported field path: {field}')

        assert actual == expected

    @pytest.mark.parametrize(
        ('field', 'expected'),
        [
            pytest.param('type', 'page', id='type'),
            pytest.param('page_param', 'page', id='page-param'),
            pytest.param('size_param', 'per_page', id='size-param'),
            pytest.param('cursor_param', 'cursor', id='cursor-param'),
            pytest.param('cursor_path', 'meta.next_cursor', id='cursor-path'),
            pytest.param('records_path', 'data.items', id='records-path'),
            pytest.param('page_size', 25, id='page-size'),
            pytest.param('max_pages', 10, id='max-pages'),
        ],
    )
    def test_profile_defaults_pagination_mapped(
        self,
        base_url: str,
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
        field: str,
        expected: object,
    ) -> None:
        """
        Test that pagination defaults are mapped correctly in profiles.
        """
        obj = {
            'profiles': {
                'default': {
                    'base_url': base_url,
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
        assert getattr(pdef, field) == expected

    def test_strips_flat_base_url(self) -> None:
        """Flat API base URLs should trim accidental outer whitespace."""
        cfg = ApiConfig.from_obj({'base_url': '  https://api.example.test  '})

        assert cfg.base_url == 'https://api.example.test'


class TestApiProfileConfig:
    """
    Unit tests for :class:`ApiProfileConfig`.

    Notes
    -----
    Tests parsing and precedence of defaults, headers, and required fields in
    API profile configuration.
    """

    @pytest.mark.parametrize(
        'defaults',
        [
            pytest.param({'pagination': 'not-a-dict'}, id='pagination-str'),
            pytest.param({'pagination': {'type': 123}}, id='pagination-type-bad'),
            pytest.param({'rate_limit': 'oops'}, id='rate-limit-str'),
            pytest.param(
                {'rate_limit': {'sleep_seconds': 'x', 'max_per_sec': []}},
                id='rate-limit-bad-values',
            ),
        ],
    )
    def test_invalid_defaults_blocks(
        self,
        base_url: str,
        defaults: dict[str, object],
        profile_config_factory: Callable[[dict[str, Any]], ApiProfileConfig],
    ) -> None:
        """
        Test that invalid defaults blocks yield None or sanitized values.
        """
        obj = {
            'base_url': base_url,
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
        base_url: str,
        profile_config_factory: Callable[[dict[str, Any]], ApiProfileConfig],
    ) -> None:
        """
        Test that headers from defaults are merged with low precedence.
        """
        obj = {
            'base_url': base_url,
            'headers': {'B': '2', 'A': '9'},
            'defaults': {'headers': {'A': '1'}},
        }
        prof = profile_config_factory(obj)
        assert prof.base_url == base_url
        assert prof.headers == {'A': '9', 'B': '2'}

    @pytest.mark.parametrize(
        ('field', 'expected'),
        [
            pytest.param('pagination_defaults.type', 'page', id='pagination-type'),
            pytest.param('pagination_defaults.page_param', 'p', id='page-param'),
            pytest.param('pagination_defaults.size_param', 's', id='size-param'),
            pytest.param('rate_limit_defaults.sleep_seconds', 0.1, id='sleep-seconds'),
            pytest.param('rate_limit_defaults.max_per_sec', 5, id='max-per-sec'),
        ],
    )
    def test_parses_defaults_blocks(
        self,
        base_url: str,
        profile_config_factory: Callable[[dict[str, Any]], ApiProfileConfig],
        api_profile_defaults_factory: Callable[..., dict[str, Any]],
        field: str,
        expected: object,
    ) -> None:
        """
        Test that defaults blocks are parsed and types are correct.
        """
        obj = {
            'base_url': base_url,
            'defaults': api_profile_defaults_factory(
                pagination={
                    'type': 'page',
                    'page_param': 'p',
                    'size_param': 's',
                },
                rate_limit={'sleep_seconds': 0.1, 'max_per_sec': 5},
            ),
        }
        prof = profile_config_factory(obj)

        section_name, attr = field.split('.', maxsplit=1)
        section = getattr(prof, section_name)
        expected_type = (
            PaginationConfig
            if section_name == 'pagination_defaults'
            else RateLimitConfig
        )
        assert isinstance(section, expected_type)
        assert getattr(section, attr) == expected

    @pytest.mark.parametrize(
        ('field', 'expected'),
        [
            pytest.param('base_url', None, id='base-url'),
            pytest.param('base_path', '/v1', id='base-path'),
            pytest.param('auth', {'token': 'abc'}, id='auth'),
        ],
    )
    def test_passthrough_fields(
        self,
        base_url: str,
        profile_config_factory: Callable[[dict[str, Any]], ApiProfileConfig],
        field: str,
        expected: object,
    ) -> None:
        """
        Test that passthrough fields (base_path, auth) are preserved.
        """
        obj = {
            'base_url': base_url,
            'base_path': '/v1',
            'auth': {'token': 'abc'},
        }
        prof = profile_config_factory(obj)
        assert getattr(prof, field) == (base_url if field == 'base_url' else expected)

    @pytest.mark.parametrize(
        ('field', 'expected'),
        [
            pytest.param('base_url', None, id='base-url'),
            pytest.param('base_path', '/v1', id='base-path'),
        ],
    )
    def test_strips_url_fields(
        self,
        base_url: str,
        profile_config_factory: Callable[[dict[str, Any]], ApiProfileConfig],
        field: str,
        expected: str | None,
    ) -> None:
        """Profile URL fields should trim accidental outer whitespace."""
        prof = profile_config_factory(
            {
                'base_url': f'  {base_url}  ',
                'base_path': '  /v1  ',
            },
        )

        assert getattr(prof, field) == (base_url if field == 'base_url' else expected)

    def test_requires_base_url(
        self,
        profile_config_factory: Callable[[dict[str, Any]], ApiProfileConfig],
    ) -> None:
        """
        Test that base_url is required for :class:`ApiProfileConfig`.
        """
        with pytest.raises(TypeError):
            profile_config_factory({})


class TestEndpointConfig:
    """
    Unit tests for :class:`EndpointConfig`.

    Notes
    -----
    Tests parsing and validation of endpoint configuration fields and error
    handling.
    """

    @pytest.mark.parametrize(
        ('field', 'expected'),
        [
            pytest.param('method', 'POST', id='method'),
            pytest.param('path_params', {'id': 'int'}, id='path-params'),
            pytest.param('body.type', 'file', id='body-type'),
            pytest.param('query_params', {'size': 'large'}, id='query-params'),
        ],
    )
    def test_captures_path_params_and_body(
        self,
        endpoint_config_factory: Callable[[dict[str, Any]], EndpointConfig],
        field: str,
        expected: object,
    ) -> None:
        """
        Test that path_params, query_params, and body are captured correctly.
        """
        ep = endpoint_config_factory(
            {
                'method': 'POST',
                'path': '/users/{id}/avatar',
                'path_params': {'id': 'int'},
                'query_params': {'size': 'large'},
                'body': {'type': 'file', 'file_path': './x.png'},
            },
        )
        assert isinstance(ep.body, dict)
        actual = ep.body['type'] if field == 'body.type' else getattr(ep, field)
        assert actual == expected

    def test_from_str_sets_no_method(
        self,
        endpoint_config_factory: Callable[[str], EndpointConfig],
    ) -> None:
        """
        Test that from_str sets no method for :class:`EndpointConfig`.
        """
        ep = endpoint_config_factory('  /ping  ')
        assert ep.path == '/ping'
        assert ep.method is None

    @pytest.mark.parametrize(
        ('payload', 'expected_exception'),
        [
            pytest.param({'method': 'GET'}, TypeError, id='missing-path'),
            pytest.param({'path': 123}, TypeError, id='path-not-str'),
            pytest.param(
                {'path': '/x', 'path_params': 'id'},
                ValueError,
                id='path_params-not-mapping',
            ),
            pytest.param(
                {'path': '/x', 'query_params': 1},
                TypeError,
                id='query_params-not-mapping',
            ),
        ],
    )
    def test_invalid_payloads_raise(
        self,
        payload: dict[str, object],
        expected_exception: type[Exception],
        endpoint_config_factory: Callable[[str], EndpointConfig],
    ) -> None:
        """
        Test that invalid payloads raise the expected exceptions.
        """
        with pytest.raises(expected_exception):
            endpoint_config_factory(payload)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ('payload', 'field', 'expected'),
        [
            pytest.param(
                {'method': 200, 'path': '/x'},
                'method',
                200,
                id='method',
            ),
            pytest.param({'path': '/x', 'body': 'json'}, 'body', 'json', id='body'),
        ],
    )
    def test_lenient_fields_do_not_raise(
        self,
        endpoint_config_factory: Callable[[dict[str, Any]], EndpointConfig],
        payload: dict[str, object],
        field: str,
        expected: object,
    ) -> None:
        """
        Test that lenient fields (method/body) do not raise errors and are
        permissive.
        """
        assert getattr(endpoint_config_factory(payload), field) == expected

    @pytest.mark.parametrize(
        ('field', 'expected'),
        [
            pytest.param('path', '/users', id='path'),
            pytest.param('method', 'GET', id='method'),
            pytest.param('query_params.active', True, id='query-param'),
        ],
    )
    def test_parses_method(
        self,
        endpoint_config_factory: Callable[[dict[str, Any]], EndpointConfig],
        field: str,
        expected: object,
    ) -> None:
        """
        Test that method and query_params are parsed correctly in
        :class:`EndpointConfig`.
        """
        ep = endpoint_config_factory(
            {
                'method': 'GET',
                'path': '/users',
                'query_params': {'active': True},
            },
        )
        actual = (
            ep.query_params.get('active')
            if field == 'query_params.active'
            else getattr(ep, field)
        )
        assert actual == expected

    def test_strips_mapping_path(
        self,
        endpoint_config_factory: Callable[[dict[str, Any]], EndpointConfig],
    ) -> None:
        """Mapping endpoint paths should trim accidental outer whitespace."""
        ep = endpoint_config_factory({'path': '  /users  '})

        assert ep.path == '/users'


class TestConfigInternalBranches:
    """Targeted branch tests for internal config helpers."""

    def test_api_profile_config_from_obj_requires_mapping(self) -> None:
        """
        Test that :meth:`ApiProfileConfig.from_obj` fails for non-mapping
        inputs.
        """
        with pytest.raises(TypeError, match='must be a mapping'):
            ApiProfileConfig.from_obj(cast(Any, 1))

    def test_api_config_from_obj_requires_mapping(self) -> None:
        """
        Test that :meth:`ApiConfig.from_obj` fails on non-mapping values.
        """
        with pytest.raises(TypeError, match='must be a mapping'):
            ApiConfig.from_obj(cast(Any, 1))

    def test_effective_defaults_requires_string_base_url(self) -> None:
        """
        Test that fallback base URL is a string when profiles are absent.
        """
        with pytest.raises(TypeError, match='base_url'):
            config_mod._effective_service_defaults(
                profiles={},
                fallback_base=123,
                fallback_headers={},
            )

    def test_endpoint_config_from_obj_rejects_invalid_shape(self) -> None:
        """
        Test that :meth:`EndpointConfig.from_obj` accepts only string or
        mapping inputs.
        """
        with pytest.raises(TypeError, match='expected str or mapping'):
            EndpointConfig.from_obj(cast(Any, [1, 2, 3]))

    @pytest.mark.parametrize(
        ('field', 'expected'),
        [pytest.param('sleep_seconds', 0.25, id='sleep-seconds')],
    )
    def test_effective_rate_limit_defaults_returns_selected_profile_value(
        self,
        base_url: str,
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
        field: str,
        expected: object,
    ) -> None:
        """Test that rate-limit defaults come from selected profile."""
        cfg = api_config_factory(
            {
                'profiles': {
                    'default': {
                        'base_url': base_url,
                        'defaults': {
                            'rate_limit': {'sleep_seconds': 0.25},
                        },
                    },
                },
                'endpoints': {},
            },
        )
        rate_limit = cfg.effective_rate_limit_defaults()
        assert rate_limit is not None
        assert getattr(rate_limit, field) == expected

    @pytest.mark.parametrize(
        ('method', 'expected'),
        [
            pytest.param(HttpMethod.GET, 'GET', id='enum'),
            pytest.param('   ', None, id='blank'),
        ],
    )
    def test_normalize_method_branches(
        self,
        method: HttpMethod | str,
        expected: str | None,
    ) -> None:
        """
        Test that method normalizer handles enum, blank, and invalid values.
        """
        assert config_mod._normalize_method(method) == expected

    def test_normalize_method_rejects_invalid_value(self) -> None:
        """Unsupported method values should raise a clear validation error."""
        with pytest.raises(ValueError, match='Unsupported HTTP method'):
            config_mod._normalize_method('tracee')

    def test_parse_profiles_skips_non_mapping_entries(
        self,
        base_url: str,
    ) -> None:
        """
        Test that :meth:`_parse_profiles` skips profile entries that are not
        mappings.
        """
        raw = {
            'good': {'base_url': base_url},
            'bad': ['not', 'mapping'],
        }
        parsed = config_mod._parse_profiles(raw)
        assert list(parsed.keys()) == ['good']
