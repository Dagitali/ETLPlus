"""
tests.unit.config.test_endpointconfig unit tests module.


Unit tests for the ETLPlus REST API endpoint configuration models.

Notes
-----
Negative and positive parsing scenarios for `EndpointConfig` including
validation of required keys and type expectations.
"""
from __future__ import annotations

import pytest


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
