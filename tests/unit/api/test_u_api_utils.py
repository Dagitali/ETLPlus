"""
:mod:`tests.unit.api.test_u_api_utils` module.

Unit tests for :mod:`etlplus.api.utils`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

from etlplus.api import PagePaginationConfigMap
from etlplus.api import PaginationConfig
from etlplus.api import PaginationType
from etlplus.api import utils

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


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
    """Unit test suite for ``build_pagination_cfg()``."""

    def test_cursor_config_without_base(self) -> None:
        """Test building cursor-based pagination config without base config."""
        overrides = {
            'type': 'cursor',
            'cursor_param': 'token',
            'cursor_path': 'meta.next',
            'page_size': 42,
        }

        cfg_map = utils.build_pagination_cfg(None, overrides)

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
        assert utils.build_pagination_cfg(None, None) is None

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

        cfg_map = utils.build_pagination_cfg(pagination, overrides)
        assert cfg_map is not None
        page_cfg = cast(PagePaginationConfigMap, cfg_map)

        assert page_cfg['type'] == 'page'
        assert page_cfg['records_path'] == 'records'
        assert page_cfg['page_param'] == 'page'
        assert page_cfg['size_param'] == 'sz'
        assert page_cfg['max_pages'] == 5
        assert page_cfg['page_size'] == 10


class TestBuildSession:
    """Unit test suite for :func:`build_session`."""

    def test_applies_configuration(self) -> None:
        """Test that session is built with given configuration."""
        sess = utils.build_session(
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
    """Unit test suite for :func:`compose_api_request_env`."""

    def test_merges_endpoint_defaults_and_overrides(
        self,
        base_url: str,
    ) -> None:
        """Test merging endpoint defaults with overrides."""
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

        env = utils.compose_api_request_env(cfg, source, overrides)

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
        pagination_cfg = cast(PagePaginationConfigMap, env['pagination'])
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
            utils.compose_api_request_env(cfg, source, None)

    def test_missing_endpoint_raises(self) -> None:
        """Test that missing endpoint raises a ValueError."""
        cfg = SimpleNamespace(apis={'core': SimpleNamespace(endpoints={})})
        source = SimpleNamespace(api='core', endpoint='ghost')
        with pytest.raises(ValueError, match='Endpoint "ghost" not defined'):
            utils.compose_api_request_env(cfg, source, None)


class TestComposeApiTargetEnv:
    """Unit test suite for :func:`compose_api_target_env`."""

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

        env = utils.compose_api_target_env(cfg, target, overrides)

        assert env['url'] == f'{base_url}/v1/users'
        assert env['method'] == 'put'
        assert env['headers']['Accept'] == 'application/json'
        assert env['headers']['Target'] == '1'
        assert env['headers']['X-Override'] == '1'
        assert env['timeout'] == 3.5
        assert env['session'] is not None


class TestComputeRlSleepSeconds:
    """Unit test suite for :func:`compute_rl_sleep_seconds`."""

    def test_defaults_when_missing(self) -> None:
        """Test that default sleep seconds is used when missing."""
        assert utils.compute_rl_sleep_seconds(None, None) == 0.0

    def test_override_wins(self) -> None:
        """Test that override value takes precedence."""
        base = {'sleep_seconds': 0.4, 'max_per_sec': None}
        assert (
            utils.compute_rl_sleep_seconds(base, {'sleep_seconds': 0.1}) == 0.1
        )


class TestPaginateWithClient:
    """Unit test suite for :func:`paginate_with_client`."""

    def test_standard_signature(self) -> None:
        """Test pagination with standard client method signature."""

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
            PagePaginationConfigMap,
            {
                'type': 'page',
                'page_param': 'page',
                'size_param': 'per_page',
                'start_page': 1,
                'page_size': 100,
            },
        )
        result = utils.paginate_with_client(
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
        """Test pagination with underscore-prefixed client method signature."""

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
            PagePaginationConfigMap,
            {
                'type': 'page',
                'page_param': 'page',
                'size_param': 'per_page',
                'start_page': 1,
                'page_size': 100,
            },
        )
        utils.paginate_with_client(
            client,
            'users',
            {'q': '1'},
            {'X': '1'},
            5,
            page_cfg,
            None,
        )

        assert client.calls[0]['sleep_seconds'] == 0.0
