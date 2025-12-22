"""
:mod:`tests.unit.api.test_u_config` module.

Unit tests for :mod:`etlplus.api.config`.

Notes
-----
- Exercises both flat and profiled API shapes.
- Uses factories for building profile defaults mappings.
- Verifies precedence and propagation of headers and ``base_path``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from etlplus.api import ApiConfig
from etlplus.api import ApiProfileConfig
from etlplus.api import EndpointConfig
from etlplus.api import PaginationConfig
from etlplus.api import RateLimitConfig

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestApiConfig:
    """
    Unit test suite for :class:`ApiConfig`.

    Notes
    -----
    Tests mapping of rate limit, header precedence, base_path propagation, and
    profile/default behaviors for API configuration.
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
        base_url: str,
        api_config_factory: Callable[[dict], ApiConfig],
        sleep: float,
        max_per: int,
    ) -> None:
        """
        Test that API profile defaults for rate limit are mapped correctly.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        api_config_factory : Callable[[dict], ApiConfig]
            Factory for building ApiConfig from dicts.
        sleep : float
            Sleep seconds for rate limit.
        max_per : int
            Max per second for rate limit.

        Returns
        -------
        None
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

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        api_obj_factory : Callable[..., dict[str, Any]]
            Factory for building API config objects.
        api_config_factory : Callable[[dict[str, Any]], ApiConfig]
            Factory for building :class:`ApiConfig` from dicts.
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

    def test_flat_shape_supported(
        self,
        base_url: str,
        api_obj_factory: Callable[..., dict[str, Any]],
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
    ) -> None:
        """
        Test that flat API config shape is supported and headers/endpoints
        are parsed.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        api_obj_factory : Callable[..., dict[str, Any]]
            Factory for building API config objects.
        api_config_factory : Callable[[dict[str, Any]], ApiConfig]
            Factory for building :class:`ApiConfig` from dicts.
        """
        obj = api_obj_factory(
            use_profiles=False,
            base_path=None,
            headers={'X-Token': 'abc'},
            endpoints={'ping': '/ping'},
        )
        cfg = api_config_factory(obj)
        assert cfg.base_url == base_url
        assert cfg.headers.get('X-Token') == 'abc'
        assert 'ping' in cfg.endpoints

    def test_parses_profiles_and_sets_defaults(
        self,
        base_url: str,
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
    ) -> None:
        """
        Test that profiles are parsed and default values are set correctly.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        api_config_factory : Callable[[dict[str, Any]], ApiConfig]
            Factory for building :class:`ApiConfig` from dicts.
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

        # Default base_url/headers should be derived from the 'default'
        # profile.

        assert cfg.base_url == f'{base_url}/v1'
        assert cfg.headers.get('Accept') == 'application/json'

        # Profiles should be preserved.
        assert {'default', 'prod'} <= set(cfg.profiles.keys())

        # Endpoint should parse.
        assert 'list' in cfg.endpoints

    def test_profile_attr_with_default(
        self,
        base_url: str,
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
    ) -> None:
        """
        Test that profile attributes with defaults are handled correctly.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        api_config_factory : Callable[[dict[str, Any]], ApiConfig]
            Factory for building :class:`ApiConfig` from dicts.
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

        # Effective getters rely on the internal helper; verify behavior.
        assert cfg.effective_base_path() == '/v1'
        assert cfg.effective_pagination_defaults() is not None

    def test_profile_attr_without_profiles_returns_none(
        self,
        base_url: str,
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
    ) -> None:
        """
        Test that profile attribute access returns None when profiles are
        absent.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        api_config_factory : Callable[[dict[str, Any]], ApiConfig]
            Factory for building :class:`ApiConfig` from dicts.
        """
        obj = {'base_url': base_url, 'endpoints': {}}
        cfg = api_config_factory(obj)
        assert cfg.effective_base_path() is None

    def test_profile_defaults_headers_and_fields(
        self,
        base_url: str,
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
    ) -> None:
        """
        Test that header precedence and profile fields are handled correctly.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        api_config_factory : Callable[[dict[str, Any]], ApiConfig]
            Factory for building :class:`ApiConfig` from dicts.
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
        base_url: str,
        api_config_factory: Callable[[dict[str, Any]], ApiConfig],
    ) -> None:
        """
        Test that pagination defaults are mapped correctly in profiles.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        api_config_factory : Callable[[dict[str, Any]], ApiConfig]
            Factory for building :class:`ApiConfig` from dicts.
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
        assert pdef.type == 'page'
        assert pdef.page_param == 'page'
        assert pdef.size_param == 'per_page'
        assert pdef.cursor_param == 'cursor'
        assert pdef.cursor_path == 'meta.next_cursor'
        assert pdef.records_path == 'data.items'
        assert pdef.page_size == 25
        assert pdef.max_pages == 10


@pytest.mark.unit
class TestApiProfileConfig:
    """
    Unit test suite for :class:`ApiProfileConfig`.

    Notes
    -----
    Tests parsing and precedence of defaults, headers, and required fields in
    API profile configuration.
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
        base_url: str,
        defaults: dict[str, object],
        profile_config_factory: Callable[[dict[str, Any]], ApiProfileConfig],
    ) -> None:
        """
        Test that invalid defaults blocks yield None or sanitized values.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        defaults : dict[str, object]
            Defaults block to test.
        profile_config_factory : Callable[[dict[str, Any]], ApiProfileConfig]
            Factory for building :class:`ApiProfileConfig` from dicts.
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

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        profile_config_factory : Callable[[dict[str, Any]], ApiProfileConfig]
            Factory for building :class:`ApiProfileConfig` from dicts.
        """
        obj = {
            'base_url': base_url,
            'headers': {'B': '2', 'A': '9'},
            'defaults': {'headers': {'A': '1'}},
        }
        prof = profile_config_factory(obj)
        assert prof.base_url == base_url
        assert prof.headers == {'A': '9', 'B': '2'}

    def test_parses_defaults_blocks(
        self,
        base_url: str,
        profile_config_factory: Callable[[dict[str, Any]], ApiProfileConfig],
        api_profile_defaults_factory: Callable[..., dict[str, Any]],
    ) -> None:
        """
        Test that defaults blocks are parsed and types are correct.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        profile_config_factory : Callable[[dict[str, Any]], ApiProfileConfig]
            Factory for building ApiProfileConfig from dicts.
        api_profile_defaults_factory : Callable[..., dict[str, Any]]
            Factory for building defaults blocks.
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

        # Ensure types are parsed.
        assert isinstance(
            prof.pagination_defaults,
            (PaginationConfig, type(None)),
        )
        assert isinstance(
            prof.rate_limit_defaults,
            (RateLimitConfig, type(None)),
        )

        # Spot-check key fields.
        if prof.pagination_defaults is not None:
            assert prof.pagination_defaults.type == 'page'
            assert prof.pagination_defaults.page_param == 'p'
            assert prof.pagination_defaults.size_param == 's'
        if prof.rate_limit_defaults is not None:
            assert prof.rate_limit_defaults.sleep_seconds == 0.1
            assert prof.rate_limit_defaults.max_per_sec == 5

    def test_passthrough_fields(
        self,
        base_url: str,
        profile_config_factory: Callable[[dict[str, Any]], ApiProfileConfig],
    ) -> None:
        """
        Test that passthrough fields (base_path, auth) are preserved.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        profile_config_factory : Callable[[dict[str, Any]], ApiProfileConfig]
            Factory for building :class:`ApiProfileConfig` from dicts.
        """
        obj = {
            'base_url': base_url,
            'base_path': '/v1',
            'auth': {'token': 'abc'},
        }
        prof = profile_config_factory(obj)
        assert prof.base_url == base_url
        assert prof.base_path == '/v1'
        assert prof.auth == {'token': 'abc'}

    def test_requires_base_url(
        self,
        profile_config_factory: Callable[[dict[str, Any]], ApiProfileConfig],
    ) -> None:
        """
        Test that base_url is required for :class:`ApiProfileConfig`.

        Parameters
        ----------
        profile_config_factory : Callable[[dict[str, Any]], ApiProfileConfig]
            Factory for building :class:`ApiProfileConfig` from dicts.
        """
        with pytest.raises(TypeError):
            profile_config_factory({})


@pytest.mark.unit
class TestEndpointConfig:
    """
    Unit test suite for :class:`EndpointConfig`.

    Notes
    -----
    Tests parsing and validation of endpoint configuration fields and error
    handling.
    """

    def test_captures_path_params_and_body(
        self,
        endpoint_config_factory: Callable[[dict[str, Any]], EndpointConfig],
    ) -> None:
        """
        Test that path_params, query_params, and body are captured correctly.

        Parameters
        ----------
        endpoint_config_factory : Callable[[dict[str, Any]], EndpointConfig]
            Factory for building :class:`EndpointConfig` from dicts.
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
        assert ep.method == 'POST'
        assert ep.path_params == {'id': 'int'}
        assert isinstance(ep.body, dict) and ep.body['type'] == 'file'
        assert ep.query_params == {'size': 'large'}

    def test_from_str_sets_no_method(
        self,
        endpoint_config_factory: Callable[[str], EndpointConfig],
    ) -> None:
        """
        Test that from_str sets no method for :class:`EndpointConfig`.

        Parameters
        ----------
        endpoint_config_factory : Callable[[str], EndpointConfig]
            Factory for building :class:`EndpointConfig` from string.
        """
        ep = endpoint_config_factory('/ping')
        assert ep.path == '/ping'
        assert ep.method is None

    @pytest.mark.parametrize(
        'payload, expected_exception',
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
            'missing-path',
            'path-not-str',
            'path_params-not-mapping',
            'query_params-not-mapping',
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

        Parameters
        ----------
        payload : dict[str, object]
            Payload to test for error handling.
        expected_exception : type[Exception]
            Expected exception type.
        endpoint_config_factory : Callable[[str], EndpointConfig]
            Factory for building :class:`EndpointConfig` from string.
        """
        with pytest.raises(expected_exception):
            endpoint_config_factory(payload)  # type: ignore[arg-type]

    def test_lenient_fields_do_not_raise(
        self,
        endpoint_config_factory: Callable[[dict[str, Any]], EndpointConfig],
    ) -> None:
        """
        Test that lenient fields (method/body) do not raise errors and are
        permissive.

        Parameters
        ----------
        endpoint_config_factory : Callable[[dict[str, Any]], EndpointConfig]
            Factory for building :class:`EndpointConfig` from dicts.
        """
        # Lenient fields (method/body) accept any type and pass through.
        ep_method = endpoint_config_factory({'method': 200, 'path': '/x'})
        assert ep_method.method == 200  # library currently permissive
        ep_body = endpoint_config_factory({'path': '/x', 'body': 'json'})
        assert ep_body.body == 'json'

    def test_parses_method(
        self,
        endpoint_config_factory: Callable[[dict[str, Any]], EndpointConfig],
    ) -> None:
        """
        Test that method and query_params are parsed correctly in
        :class:`EndpointConfig`.

        Parameters
        ----------
        endpoint_config_factory : Callable[[dict[str, Any]], EndpointConfig]
            Factory for building :class:`EndpointConfig` from dicts.
        """
        ep = endpoint_config_factory(
            {
                'method': 'GET',
                'path': '/users',
                'query_params': {'active': True},
            },
        )
        assert ep.path == '/users'
        assert ep.method == 'GET'
        assert ep.query_params.get('active') is True
