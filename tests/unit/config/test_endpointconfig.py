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

from etlplus.config import EndpointConfig


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
