"""
ETLPlus Config Tests
====================

Unit tests for the ETLPlus configuration models.

Notes
-----
These tests cover the loading and parsing of pipeline configuration
YAML files, including variable substitution and profile handling.
"""
from __future__ import annotations

from etlplus.config import ApiConfig
from etlplus.config import EndpointConfig
from etlplus.config import PipelineConfig


def test_api_config_parses_profiles_and_sets_defaults():
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
        'endpoints': {
            'list': {'path': '/items'},
        },
    }

    cfg = ApiConfig.from_obj(obj)

    # Default base_url/headers should be derived from the 'default' profile
    assert cfg.base_url == 'https://api.example.com/v1'
    assert cfg.headers.get('Accept') == 'application/json'
    # Profiles should be preserved
    assert 'default' in cfg.profiles and 'prod' in cfg.profiles
    # Endpoint should parse
    assert 'list' in cfg.endpoints


def test_api_config_flat_shape_still_supported():
    obj = {
        'base_url': 'https://flat.example.com',
        'headers': {'X-Token': 'abc'},
        'endpoints': {'ping': '/ping'},
    }

    cfg = ApiConfig.from_obj(obj)
    assert cfg.base_url == 'https://flat.example.com'
    assert cfg.headers.get('X-Token') == 'abc'
    assert 'ping' in cfg.endpoints


def test_api_profile_defaults_headers_and_fields():
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

    cfg = ApiConfig.from_obj(obj)
    # Headers: defaults.headers < profile.headers < top-level
    assert cfg.headers['Accept'] == 'application/json'
    assert cfg.headers['Authorization'] == 'Bearer token'
    # profile.headers overrides defaults
    assert cfg.headers['X-From-Defaults'] == '2'
    # top-level overrides/augments
    assert cfg.headers['X-Top'] == 't'

    # Profile extras captured
    prof = cfg.profiles['default']
    assert prof.base_path == '/v1'
    assert prof.auth.get('type') == 'bearer'


def test_api_profile_defaults_pagination_mapped():
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

    cfg = ApiConfig.from_obj(obj)
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


def test_api_profile_defaults_rate_limit_mapped():
    obj = {
        'profiles': {
            'default': {
                'base_url': 'https://api.example.com',
                'defaults': {
                    'rate_limit': {
                        'sleep_seconds': 0.5,
                        'max_per_sec': 2,
                    },
                },
            },
        },
        'endpoints': {},
    }

    cfg = ApiConfig.from_obj(obj)
    prof = cfg.profiles['default']
    rdef = getattr(prof, 'rate_limit_defaults', None)
    assert rdef is not None
    assert rdef.sleep_seconds == 0.5
    assert rdef.max_per_sec == 2


def test_endpoint_captures_path_params_and_body():
    ep = EndpointConfig.from_obj({
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


def test_api_config_effective_base_url_and_build_endpoint_url():
    obj = {
        'profiles': {
            'default': {
                'base_url': 'https://api.example.com',
                'base_path': '/v1',
            },
        },
        'endpoints': {
            'users': {'path': '/users'},
        },
    }

    cfg = ApiConfig.from_obj(obj)

    # Effective base URL composes base_url + base_path
    assert cfg.effective_base_url() == 'https://api.example.com/v1'

    # build_endpoint_url composes base and endpoint path
    ep = cfg.endpoints['users']
    url = cfg.build_endpoint_url(ep)
    assert url == 'https://api.example.com/v1/users'


def test_endpoint_config_parses_method():
    ep = EndpointConfig.from_obj({
        'method': 'GET',
        'path': '/users',
        'query_params': {'active': True},
    })
    assert ep.path == '/users'
    assert ep.method == 'GET'
    assert ep.query_params.get('active') is True


def test_endpoint_config_from_str_sets_no_method():
    ep = EndpointConfig.from_obj('/ping')
    assert ep.path == '/ping'
    assert ep.method is None


def test_from_yaml_includes_profile_env_in_substitution(tmp_path):
    yml = (
        """
name: Test
profile:
  env:
    FOO: bar
vars:
  X: 123
sources:
  - name: s
    type: file
    format: json
    path: "${FOO}-${X}.json"
targets: []
jobs: []
"""
    ).strip()

    p = tmp_path / 'cfg.yml'
    p.write_text(yml, encoding='utf-8')

    cfg = PipelineConfig.from_yaml(p, substitute=True, env={})
    # After substitution, re-parse should keep the resolved path
    s = next(s for s in cfg.sources if s.name == 's')
    assert getattr(s, 'path', None) == 'bar-123.json'
