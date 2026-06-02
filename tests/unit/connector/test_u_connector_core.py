"""
:mod:`tests.unit.connector.test_u_connector_core` module.

Unit tests for :mod:`etlplus.connector._core`.
"""

from __future__ import annotations

import pytest

from etlplus.connector._api import ConnectorApi
from etlplus.connector._core import ConnectorProtocol
from etlplus.connector._file import ConnectorFile

from .pytest_connector_support import CONNECTOR_CLASS_PARAMS
from .pytest_connector_support import ConnectorClass

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorProtocol:
    """Unit tests for connector protocol behavior."""

    @pytest.mark.parametrize(
        'connector_cls',
        CONNECTOR_CLASS_PARAMS,
    )
    def test_concrete_connector_satisfies_runtime_protocol(
        self,
        connector_cls: ConnectorClass,
    ) -> None:
        """
        Test that concrete connector dataclasses satisfy the runtime protocol.
        """
        connector = connector_cls.from_obj({'name': 'connector'})

        assert isinstance(connector, ConnectorProtocol)

    def test_protocol_placeholder_from_obj_raises_not_implemented(self) -> None:
        """The protocol placeholder should fail closed when called directly."""
        with pytest.raises(NotImplementedError):
            ConnectorProtocol.from_obj(  # pyright: ignore[reportAbstractUsage]
                {'name': 'payload'},
            )


class TestConnectorBaseContracts:
    """Shared contract tests for concrete connector base subclasses."""

    def test_dict_field_copies_mapping_values(self) -> None:
        """Mapping-like fields should be returned as plain mutable dicts."""
        options = {'encoding': 'utf-8'}
        parsed = ConnectorFile._dict_field({'options': options}, 'options')

        assert parsed == {'encoding': 'utf-8'}
        assert parsed is not options

    def test_dict_field_defaults_non_mapping_values(self) -> None:
        """Non-mapping optional fields should normalize to an empty dict."""
        assert ConnectorFile._dict_field({'options': ['invalid']}, 'options') == {}

    def test_optional_str_coerces_scalar_values_to_strings(self) -> None:
        """Scalar optional field values should normalize to strings."""
        assert ConnectorFile._optional_str({'path': True}, 'path') == 'True'

    def test_optional_str_keeps_single_blank_field_absent(self) -> None:
        """A single blank optional string field should normalize to ``None``."""
        assert ConnectorFile._optional_str({'path': '   '}, 'path') is None

    def test_optional_str_returns_trimmed_first_present_value(self) -> None:
        """Optional string parsing should trim the first present string value."""
        assert (
            ConnectorApi._optional_str(
                {'api': '  service-name  ', 'service': 'fallback'},
                'api',
                'service',
            )
            == 'service-name'
        )

    def test_optional_str_skips_blank_when_multiple_fields_are_available(
        self,
    ) -> None:
        """Blank preferred fields should fall through to aliases."""
        assert (
            ConnectorApi._optional_str(
                {'api': '   ', 'service': '  fallback  '},
                'api',
                'service',
            )
            == 'fallback'
        )

    @pytest.mark.parametrize(
        'connector_cls',
        CONNECTOR_CLASS_PARAMS,
    )
    @pytest.mark.parametrize(
        'name_fields',
        [
            pytest.param({}, id='missing-name'),
            pytest.param({'name': None}, id='non-string-name'),
            pytest.param({'name': ''}, id='empty-name'),
            pytest.param({'name': '   '}, id='whitespace-name'),
        ],
    )
    def test_requires_name(
        self,
        connector_cls: ConnectorClass,
        name_fields: dict[str, object],
    ) -> None:
        """Connector constructors should reject missing or invalid names."""
        with pytest.raises(
            TypeError,
            match=f'{connector_cls.__name__} requires a "name"',
        ):
            connector_cls.from_obj(name_fields)

    @pytest.mark.parametrize(
        'connector_cls',
        CONNECTOR_CLASS_PARAMS,
    )
    def test_strips_name(
        self,
        connector_cls: ConnectorClass,
    ) -> None:
        """Connector constructors should strip accidental name whitespace."""
        connector = connector_cls.from_obj({'name': '  connector  '})

        assert connector.name == 'connector'

    def test_str_dict_field_coerces_mapping_keys_and_values(self) -> None:
        """String dictionary fields should coerce keys and values to strings."""
        headers = ConnectorApi._str_dict_field(
            {'headers': {'Accept': 'application/json', 1: 2}},
            'headers',
        )

        assert headers == {'Accept': 'application/json', '1': '2'}

    def test_str_dict_field_defaults_non_mapping_values(self) -> None:
        """Non-mapping string dictionary fields should normalize to an empty dict."""
        assert ConnectorApi._str_dict_field({'headers': ['invalid']}, 'headers') == {}
