"""
tests.unit.config.test_endpointconfig unit tests module.


Unit tests for the ETLPlus configuration models.

Notes
-----
These tests cover the loading and parsing of pipeline configuration
YAML files, including variable substitution and profile handling.
"""
from __future__ import annotations


# SECTION: TESTS ============================================================ #


class TestEndpointConfig:
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

    def test_from_str_sets_no_method(
        self,
        endpoint_config_factory,
    ) -> None:  # noqa: D401
        ep = endpoint_config_factory('/ping')
        assert ep.path == '/ping'
        assert ep.method is None
