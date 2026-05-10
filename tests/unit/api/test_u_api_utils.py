"""
:mod:`tests.unit.api.test_u_api_utils` module.

Unit tests for :mod:`etlplus.api._utils`.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

from etlplus.api import ApiConfig
from etlplus.api import CursorPaginationConfigDict
from etlplus.api import EndpointConfig
from etlplus.api import PagePaginationConfigDict
from etlplus.api import PaginationConfig
from etlplus.api import PaginationType
from etlplus.api import RateLimitConfig
from etlplus.api import _utils

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


class _ApiCfg:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.headers = {'Accept': 'application/json'}
        self.endpoints = {'users': _Endpoint()}
        self.retry = {'max_attempts': 1}
        self.retry_network_errors = False
        self.session = {'headers': {'Api': '1'}}

    def build_endpoint_url(self, ep: _Endpoint) -> str:
        """ "Build full URL for the given endpoint."""
        return f'{self.base_url}/v1{ep.path}'

    def effective_base_path(self) -> str:
        """Get the effective base path for the API."""
        return '/v1'

    def effective_pagination_defaults(self) -> dict[str, Any]:
        """Get the effective pagination defaults for the API."""
        return {'type': 'cursor', 'cursor_param': 'next'}

    def effective_rate_limit_defaults(self) -> dict[str, Any]:
        """Get the effective rate limit defaults for the API."""
        return {'sleep_seconds': 0.25}


class _Endpoint:
    def __init__(self) -> None:
        self.path = '/users'
        self.query_params = {'fields': 'id,name'}
        # self.pagination = SimpleNamespace(
        self.pagination = PaginationConfig(
            # type='page',
            type=PaginationType.PAGE,
            records_path='data.items',
            max_pages=5,
            max_records=200,
            page_param='p',
            size_param='s',
            start_page=2,
            page_size=25,
        )
        self.rate_limit = {'sleep_seconds': 0.4}
        self.retry = {'max_attempts': 2}
        self.retry_network_errors = False
        self.session = {'headers': {'Endpoint': '1'}}
        self.headers = {'Endpoint': '1'}


# SECTION: TESTS ============================================================ #


class TestBuildPaginationCfg:
    """Unit tests for :func:`build_pagination_cfg`."""

    def test_cursor_config_without_base(self) -> None:
        """
        Test building cursor-based pagination config without a base config.
        """
        overrides = {
            'type': 'cursor',
            'cursor_param': 'token',
            'cursor_path': 'meta.next',
            'page_size': 42,
        }

        cfg_map = _utils.build_pagination_cfg(None, overrides)

        assert cfg_map == {
            'type': 'cursor',
            'records_path': None,
            'max_pages': None,
            'max_records': None,
            'cursor_param': 'token',
            'cursor_path': 'meta.next',
            'page_size': 42,
            'start_cursor': None,
        }

    def test_missing_type_returns_none(self) -> None:
        """Test that missing pagination type returns ``None``."""
        assert _utils.build_pagination_cfg(None, None) is None

    def test_page_config_with_overrides(self) -> None:
        """Test building page-based pagination config with overrides."""
        pagination = PaginationConfig(
            type=PaginationType.PAGE,
            records_path='records',
            max_pages=2,
            max_records=50,
            page_param='pg',
            size_param='sz',
            start_page=3,
            page_size=10,
        )
        overrides = {'max_pages': 5, 'page_param': 'page'}

        cfg_map = _utils.build_pagination_cfg(pagination, overrides)
        assert cfg_map is not None
        page_cfg = cast(PagePaginationConfigDict, cfg_map)

        assert page_cfg['type'] == 'page'
        assert page_cfg['records_path'] == 'records'
        assert page_cfg['page_param'] == 'page'
        assert page_cfg['size_param'] == 'sz'
        assert page_cfg['max_pages'] == 5
        assert page_cfg['page_size'] == 10


class TestBuildSession:
    """Unit tests for :func:`build_session`."""

    def test_applies_configuration(self) -> None:
        """Test that session is built with given configuration."""
        sess = _utils.build_session(
            {
                'headers': {'X': '1'},
                'params': {'debug': '1'},
                'auth': ('user', 'pass'),
                'verify': False,
                'cert': 'cert.pem',
                'proxies': {'https': 'proxy'},
                'cookies': {'a': 'b'},
                'trust_env': False,
            },
        )

        assert sess.headers['X'] == '1'
        assert sess.params == {'debug': '1'}
        assert sess.auth == ('user', 'pass')
        assert sess.verify is False
        assert sess.cert == 'cert.pem'
        assert sess.proxies['https'] == 'proxy'
        assert sess.cookies.get('a') == 'b'


class TestComposeApiRequestEnv:
    """Unit tests for :func:`compose_api_request_env`."""

    def test_merges_endpoint_defaults_and_overrides(
        self,
        base_url: str,
    ) -> None:
        """Test that merging endpoint defaults with overrides."""
        cfg = SimpleNamespace(apis={'core': _ApiCfg(base_url)})
        source = SimpleNamespace(
            api='core',
            endpoint='users',
            query_params={'limit': 5},
            headers={'User-Agent': 'pytest'},
            pagination=None,
            rate_limit=None,
            retry=None,
            retry_network_errors=False,
            session={'headers': {'Source': '1'}},
        )
        overrides = {
            'query_params': {'search': 'ada'},
            'headers': {'X-Test': '1'},
            'timeout': 7.5,
            'pagination': {'type': 'page', 'page_param': 'page'},
            'rate_limit': {'sleep_seconds': 0.05},
            'session': {'params': {'debug': '1'}},
            'retry': {'max_attempts': 4},
            'retry_network_errors': True,
        }

        env = _utils.compose_api_request_env(cfg, source, overrides)

        assert env['use_endpoints'] is True
        assert env['base_url'] == base_url
        assert env['endpoint_key'] == 'users'
        assert env['params'] == {
            'fields': 'id,name',
            'limit': 5,
            'search': 'ada',
        }
        assert env['headers']['Accept'] == 'application/json'
        assert env['headers']['User-Agent'] == 'pytest'
        assert env['headers']['X-Test'] == '1'
        assert env['timeout'] == 7.5
        assert env['pagination'] is not None
        pagination_cfg = cast(PagePaginationConfigDict, env['pagination'])
        assert pagination_cfg['type'] == 'page'
        assert env['sleep_seconds'] == 0.05
        assert env['retry'] == {'max_attempts': 4}
        assert env['retry_network_errors'] is True
        assert env['session'] is not None

    def test_missing_api_raises(self) -> None:
        """Test that missing API raises a ValueError."""
        cfg = SimpleNamespace(apis={})
        source = SimpleNamespace(api='missing', endpoint='users')
        with pytest.raises(ValueError, match='API not defined'):
            _utils.compose_api_request_env(cfg, source, None)

    def test_missing_endpoint_raises(self) -> None:
        """Test that missing endpoint raises a ValueError."""
        cfg = SimpleNamespace(apis={'core': SimpleNamespace(endpoints={})})
        source = SimpleNamespace(api='core', endpoint='ghost')
        with pytest.raises(ValueError, match='Endpoint "ghost" not defined'):
            _utils.compose_api_request_env(cfg, source, None)


class TestComposeApiTargetEnv:
    """Unit tests for :func:`compose_api_target_env`."""

    def test_inherits_api_defaults_when_url_missing(
        self,
        base_url: str,
    ) -> None:
        """Test that API defaults are inherited when URL is missing."""
        cfg = SimpleNamespace(apis={'core': _ApiCfg(base_url)})
        target = SimpleNamespace(
            api='core',
            endpoint='users',
            headers={'Target': '1'},
        )
        overrides = {
            'headers': {'X-Override': '1'},
            'method': 'put',
            'timeout': 3.5,
            'session': {'headers': {'Auth': 'token'}},
        }

        env = _utils.compose_api_target_env(cfg, target, overrides)

        assert env['url'] == f'{base_url}/v1/users'
        assert env['method'] == 'put'
        assert env['headers']['Accept'] == 'application/json'
        assert env['headers']['Target'] == '1'
        assert env['headers']['X-Override'] == '1'
        assert env['timeout'] == 3.5
        assert env['session'] is not None


class TestComputeRlSleepSeconds:
    """Unit tests for :func:`compute_rl_sleep_seconds`."""

    def test_defaults_when_missing(self) -> None:
        """Test that default sleep seconds is used when missing."""
        assert _utils.compute_rl_sleep_seconds(None, None) == 0.0

    def test_override_wins(self) -> None:
        """Test that override value takes precedence."""
        base = {'sleep_seconds': 0.4, 'max_per_sec': None}
        assert _utils.compute_rl_sleep_seconds(base, {'sleep_seconds': 0.1}) == 0.1


class TestPaginateWithClient:
    """Unit tests for :func:`paginate_with_client`."""

    def test_standard_signature(self) -> None:
        """Test that pagination with standard client method signature."""

        class Client:
            """Dummy client with standard paginate method."""

            def __init__(self) -> None:
                self.calls: list[dict[str, Any]] = []

            def paginate(
                self,
                endpoint_key: str,
                *,
                params: Any,
                headers: Any,
                timeout: Any,
                pagination: Any,
                sleep_seconds: Any,
            ) -> list[str]:
                """Record call parameters."""
                self.calls.append(
                    {
                        'endpoint_key': endpoint_key,
                        'params': params,
                        'headers': headers,
                        'timeout': timeout,
                        'pagination': pagination,
                        'sleep_seconds': sleep_seconds,
                    },
                )
                return ['ok']

        client = Client()
        page_cfg = cast(
            PagePaginationConfigDict,
            {
                'type': 'page',
                'page_param': 'page',
                'size_param': 'per_page',
                'start_page': 1,
                'page_size': 100,
            },
        )
        result = _utils.paginate_with_client(
            client,
            'users',
            {'q': '1'},
            {'X': '1'},
            5,
            page_cfg,
            0.3,
        )

        assert result == ['ok']
        assert client.calls[0]['sleep_seconds'] == 0.3

    def test_underscore_signature(self) -> None:
        """
        Test that pagination with underscore-prefixed client method signature.
        """

        class Client:
            """Dummy client with underscore-prefixed paginate method."""

            def __init__(self) -> None:
                self.calls: list[dict[str, Any]] = []

            def paginate(
                self,
                endpoint_key: str,
                *,
                _params: Any,
                _headers: Any,
                _timeout: Any,
                pagination: Any,
                _sleep_seconds: Any,
            ) -> list[str]:
                """Record call parameters."""
                self.calls.append(
                    {
                        'endpoint_key': endpoint_key,
                        'params': _params,
                        'headers': _headers,
                        'timeout': _timeout,
                        'pagination': pagination,
                        'sleep_seconds': _sleep_seconds,
                    },
                )
                return ['ok']

        client = Client()
        page_cfg = cast(
            PagePaginationConfigDict,
            {
                'type': 'page',
                'page_param': 'page',
                'size_param': 'per_page',
                'start_page': 1,
                'page_size': 100,
            },
        )
        _utils.paginate_with_client(
            client,
            'users',
            {'q': '1'},
            {'X': '1'},
            5,
            page_cfg,
            None,
        )

        assert client.calls[0]['sleep_seconds'] == 0.0


class TestUtilsInternalBranches:
    """Branch-focused tests for internal API utility helpers."""

    def test_build_endpoint_client_helper(self) -> None:
        """
        Test that :func:`build_endpoint_client` wires env options into
        :class:`EndpointClient`.
        """
        client = _utils.build_endpoint_client(
            base_url='https://example.test',
            base_path='/v1',
            endpoints={'users': '/users'},
            env={'retry_network_errors': True},
        )
        assert client.base_url == 'https://example.test'
        assert client.base_path == '/v1'
        assert client.endpoints['users'] == '/users'
        assert client.retry_network_errors is True

    def test_build_pagination_cfg_page_cursor_and_unknown_variants(
        self,
    ) -> None:
        """
        Test that :func:`build_pagination_cfg` covers page/cursor/unknown type
        branches.
        """
        raw_page_cfg = _utils.build_pagination_cfg(None, {'type': 'page'})
        assert raw_page_cfg is not None
        page_cfg = cast(PagePaginationConfigDict, raw_page_cfg)
        assert page_cfg['type'] == 'page'
        assert page_cfg['page_param'] == 'page'
        assert page_cfg['size_param'] == 'per_page'

        cursor_base = PaginationConfig(
            type=PaginationType.CURSOR,
            cursor_param='token',
            cursor_path='meta.next',
            page_size=50,
            start_cursor='abc',
        )
        raw_cursor_cfg = _utils.build_pagination_cfg(
            cursor_base,
            {'type': 'cursor'},
        )
        assert raw_cursor_cfg is not None
        cursor_cfg = cast(CursorPaginationConfigDict, raw_cursor_cfg)
        assert cursor_cfg is not None
        assert cursor_cfg['type'] == 'cursor'
        assert cursor_cfg['cursor_param'] == 'token'
        assert cursor_cfg['cursor_path'] == 'meta.next'
        assert cursor_cfg['page_size'] == 50
        assert cursor_cfg['start_cursor'] == 'abc'

        unknown = _utils.build_pagination_cfg(None, {'type': 'custom'})
        assert unknown is not None
        assert unknown['type'] == 'custom'

    def test_build_session_catches_params_and_cookie_update_exceptions(
        self,
    ) -> None:
        """Test that param/cookie update exceptions are swallowed."""

        class _BrokenMapping(Mapping[str, Any]):
            def __getitem__(self, key: str) -> Any:
                raise KeyError(key)

            def __iter__(self):
                raise TypeError('bad iter')

            def __len__(self) -> int:
                return 0

        class _BadCookies(Mapping[str, Any]):
            def __getitem__(self, key: str) -> Any:
                raise ValueError(key)

            def __iter__(self):
                return iter(['k'])

            def __len__(self) -> int:
                return 1

            def items(self):
                raise ValueError('bad cookies')

        session = _utils.build_session(
            {
                'params': cast(Any, _BrokenMapping()),
                'cookies': cast(Any, _BadCookies()),
            },
        )
        assert session is not None

    def test_build_session_handles_error_tolerant_branches(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that session builder tolerates assignment/update failures."""

        class _BadMap(dict[str, Any]):
            def __iter__(self):  # type: ignore[override]
                raise TypeError('bad iter')

        class _BadCookies(dict[str, Any]):
            def items(self):  # type: ignore[override]
                raise ValueError('bad cookies')

        class _TinySession:
            def __init__(self) -> None:
                self.headers: dict[str, Any] = {}
                self.params: dict[str, Any] = {}
                self.auth: Any = None
                self.verify: Any = True
                self.cert: Any = None
                self.proxies: dict[str, Any] = {}
                self.cookies: dict[str, Any] = {}

            def __setattr__(self, name: str, value: Any) -> None:
                if name == 'trust_env':
                    raise AttributeError('no trust_env')
                object.__setattr__(self, name, value)

        monkeypatch.setattr(_utils.requests, 'Session', _TinySession)

        session = _utils.build_session(
            {
                'headers': cast(Any, ['not-mapping']),
                'params': cast(Any, _BadMap({'a': 1})),
                'auth': 'token-auth',
                'cookies': cast(Any, _BadCookies({'x': '1'})),
                'trust_env': True,
            },
        )
        assert isinstance(session, _TinySession)
        assert session.auth == 'token-auth'

        default_session = _utils.build_session(None)
        assert isinstance(default_session, _TinySession)

    def test_compose_api_request_env_falls_back_to_api_level_defaults(
        self,
        base_url: str,
    ) -> None:
        """Test that request composition falls back from endpoint to API defaults."""
        cfg = SimpleNamespace(apis={'core': _ApiCfg(base_url)})
        endpoint = cfg.apis['core'].endpoints['users']
        endpoint.pagination = None
        endpoint.rate_limit = None
        endpoint.retry = None
        endpoint.retry_network_errors = None

        source = SimpleNamespace(
            api='core',
            endpoint='users',
            query_params=None,
            headers=None,
            pagination=None,
            rate_limit=None,
            retry=None,
            retry_network_errors=None,
            session=None,
        )

        env = _utils.compose_api_request_env(cfg, source, {})

        assert env['pagination'] is None
        assert env['sleep_seconds'] == 0.25
        assert env['retry'] == {'max_attempts': 1}
        assert env['retry_network_errors'] is False

    def test_compose_api_request_env_preserves_source_pagination_and_rate_limit(
        self,
        base_url: str,
    ) -> None:
        """Test that source pagination and rate-limit values take precedence."""
        cfg = SimpleNamespace(apis={'core': _ApiCfg(base_url)})
        source = SimpleNamespace(
            api='core',
            endpoint='users',
            query_params=None,
            headers=None,
            pagination=PaginationConfig(
                type=PaginationType.PAGE,
                page_param='page',
                page_size=10,
            ),
            rate_limit=RateLimitConfig(sleep_seconds=0.1),
            retry=None,
            retry_network_errors=None,
            session=None,
        )

        env = _utils.compose_api_request_env(cfg, source, {})

        assert env['pagination'] is not None
        pagination_cfg = cast(PagePaginationConfigDict, env['pagination'])
        assert pagination_cfg['type'] == 'page'
        assert pagination_cfg['page_param'] == 'page'
        assert pagination_cfg['page_size'] == 10
        assert env['sleep_seconds'] == 0.1

    def test_compose_api_request_env_without_api_reference(self) -> None:
        """
        Test that :func:`compose_api_request_env` works without API/endpoint
        linkage.
        """
        cfg = SimpleNamespace(apis={})
        source = SimpleNamespace(
            url='https://example.test/items',
            query_params={'a': 1},
            headers={'H': '1'},
            pagination=None,
            rate_limit={'sleep_seconds': 0.4},
            retry={'max_attempts': 1},
            retry_network_errors=False,
            session=None,
            api=None,
            endpoint=None,
        )
        env = _utils.compose_api_request_env(
            cfg,
            source,
            {
                'retry': {'max_attempts': 2},
                'retry_network_errors': True,
                'session': ['not', 'mapping'],
                'query_params': cast(Any, ['ignored']),
                'headers': cast(Any, ['ignored']),
            },
        )
        assert env['use_endpoints'] is False
        assert env['url'] == 'https://example.test/items'
        assert env['retry'] == {'max_attempts': 2}
        assert env['retry_network_errors'] is True

    def test_compose_api_request_env_without_retry_overrides(
        self,
        base_url: str,
    ) -> None:
        """
        Test that absent retry override keys preserves source retry settings.
        """
        cfg = SimpleNamespace(apis={'core': _ApiCfg(base_url)})
        source = SimpleNamespace(
            api='core',
            endpoint='users',
            query_params=None,
            headers=None,
            pagination=None,
            rate_limit=None,
            retry={'max_attempts': 9},
            retry_network_errors=False,
            session=None,
        )
        env = _utils.compose_api_request_env(cfg, source, {})
        assert env['retry'] == {'max_attempts': 9}
        assert env['retry_network_errors'] is False

    def test_compose_api_target_env_skips_inherit_when_url_given(
        self,
        base_url: str,
    ) -> None:
        """Test that explicit target URL bypasses API endpoint inheritance."""
        cfg = SimpleNamespace(apis={'core': _ApiCfg(base_url)})
        target = SimpleNamespace(
            api='core',
            endpoint='users',
            headers={'T': '1'},
        )
        env = _utils.compose_api_target_env(
            cfg,
            target,
            {'url': 'https://override.test/u'},
        )
        assert env['url'] == 'https://override.test/u'
        assert env['headers'] == {'T': '1'}

    def test_compute_rl_sleep_seconds_variants(self) -> None:
        """Test that rate-limit sleep helper filters overrides correctly."""
        rl_obj = RateLimitConfig(sleep_seconds=0.5, max_per_sec=None)
        assert _utils.compute_rl_sleep_seconds(rl_obj, {'max_per_sec': 4}) == 0.5
        assert _utils.compute_rl_sleep_seconds(None, {'max_per_sec': 4}) == 0.25
        assert _utils.compute_rl_sleep_seconds({'sleep_seconds': 0.2}, {'x': 1}) == 0.2

    def test_internal_helpers_handle_non_mapping_inputs(
        self,
        base_url: str,
    ) -> None:
        """
        Test that internal helper branches gracefully handles invalid inputs.
        """
        cfg = SimpleNamespace(apis={})
        source = SimpleNamespace(
            api=None,
            endpoint=None,
            url='https://already.test/u',
            headers={'X': '1'},
            query_params={'q': '1'},
            session=cast(Any, 'bad'),
            pagination=None,
            rate_limit=None,
            retry=None,
            retry_network_errors=False,
        )
        target_cfg = SimpleNamespace(apis={})
        target = SimpleNamespace(
            api=None,
            endpoint=None,
            url='https://target.test/u',
            headers={'T': '1'},
        )

        assert _utils.compose_api_request_env(cfg, source, {})['session'] is None
        assert (
            _utils.compose_api_target_env(
                target_cfg,
                target,
                {'session': cast(Any, 'bad')},
            )['session']
            is None
        )

        api_cfg = _ApiCfg(base_url)
        ep = _Endpoint()
        url, headers, session_cfg = _utils._inherit_http_from_api_endpoint(
            cast(ApiConfig, api_cfg),
            cast(EndpointConfig, ep),
            'https://already.test/u',
            {'X': '1'},
            {'headers': {'S': '1'}},
            force_url=False,
        )
        assert url == 'https://already.test/u'
        assert headers['Accept'] == 'application/json'
        assert headers['X'] == '1'
        assert session_cfg is not None

    def test_inherit_http_skips_non_mapping_session_inputs(
        self,
        base_url: str,
    ) -> None:
        """
        Test that session inheritance returns ``None`` when all session inputs
        are invalid.
        """
        api_cfg = _ApiCfg(base_url)
        api_cfg.session = cast(Any, 'bad')
        ep = _Endpoint()
        ep.session = cast(Any, 'bad')
        _, _, session_cfg = _utils._inherit_http_from_api_endpoint(
            cast(ApiConfig, api_cfg),
            cast(EndpointConfig, ep),
            None,
            {},
            cast(Any, 'bad'),
            force_url=False,
        )
        assert session_cfg is None

    def test_resolve_request_raises_when_method_missing(self) -> None:
        """
        Test that resolve_request fails when session lacks method callable.
        """
        with pytest.raises(TypeError, match='must supply a callable'):
            _utils.resolve_request(
                'get',
                session=SimpleNamespace(),
                timeout=1.0,
            )

    def test_resolve_request_success_path(self) -> None:
        """
        Test that resolve_request returns callable, timeout, and method enum.
        """

        class _Session:
            @staticmethod
            def post(*_args: Any, **_kwargs: Any) -> Any:
                return object()

        call, timeout, method = _utils.resolve_request(
            'post',
            session=_Session(),
            timeout=2.0,
        )
        assert callable(call)
        assert timeout == 2.0
        assert method == _utils.HttpMethod.POST
