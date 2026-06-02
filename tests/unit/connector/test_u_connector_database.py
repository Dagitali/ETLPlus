"""
:mod:`tests.unit.connector.test_u_connector_database` module.

Unit tests for :mod:`etlplus.connector._database`.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import etlplus.connector._database as database_mod
from etlplus.connector._database import ConnectorDb
from etlplus.connector._enums import DataConnectorType
from tests.pytest_shared_support import BIGQUERY_CASE
from tests.pytest_shared_support import SNOWFLAKE_CASE

from .pytest_connector_support import assert_connector_fields

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorDb:
    """Unit tests for :class:`ConnectorDb`."""

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            pytest.param(
                BIGQUERY_CASE.connector_payload(
                    use_alias=True,
                    name='warehouse',
                    connection_string='sqlite:///warehouse.db',
                    query='select * from events',
                    table='events',
                    mode='append',
                ),
                BIGQUERY_CASE.expected_connector_attrs(
                    name='warehouse',
                    connection_string='sqlite:///warehouse.db',
                    query='select * from events',
                    table='events',
                    mode='append',
                )
                | {'type': DataConnectorType.DATABASE},
                id='provider-normalization',
            ),
            pytest.param(
                {
                    'name': 'warehouse',
                    'type': 'database',
                    'connection_string': 123,
                    'query': False,
                    'table': 456,
                    'mode': None,
                },
                {
                    'type': DataConnectorType.DATABASE,
                    'name': 'warehouse',
                    'connection_string': '123',
                    'provider': None,
                    'project': None,
                    'dataset': None,
                    'location': None,
                    'account': None,
                    'database': None,
                    'schema': None,
                    'warehouse': None,
                    'query': 'False',
                    'table': '456',
                    'mode': None,
                },
                id='coerces-optional-strings',
            ),
            pytest.param(
                BIGQUERY_CASE.connector_payload(
                    include_provider=False,
                    omit_fields=('location',),
                    name='warehouse',
                ),
                BIGQUERY_CASE.expected_connector_attrs(
                    name='warehouse',
                    omit_fields=('location',),
                )
                | {'type': DataConnectorType.DATABASE},
                id='infers-bigquery-provider',
            ),
            pytest.param(
                BIGQUERY_CASE.connector_payload(
                    include_provider=False,
                    omit_fields=('location',),
                    name='warehouse',
                )
                | {'provider': None, 'engine': 'gcp-bigquery'},
                BIGQUERY_CASE.expected_connector_attrs(
                    name='warehouse',
                    omit_fields=('location',),
                )
                | {'type': DataConnectorType.DATABASE},
                id='engine-used-when-provider-empty',
            ),
            pytest.param(
                BIGQUERY_CASE.connector_payload(
                    include_provider=False,
                    omit_fields=('location',),
                    name='warehouse',
                )
                | {'provider': '', 'engine': 'gcp-bigquery'},
                BIGQUERY_CASE.expected_connector_attrs(
                    name='warehouse',
                    omit_fields=('location',),
                )
                | {'type': DataConnectorType.DATABASE},
                id='engine-used-when-provider-blank',
            ),
            pytest.param(
                SNOWFLAKE_CASE.connector_payload(
                    use_alias=True,
                    name='snowflake_wh',
                    table='events',
                ),
                SNOWFLAKE_CASE.expected_connector_attrs(
                    name='snowflake_wh',
                    table='events',
                )
                | {'type': DataConnectorType.DATABASE},
                id='snowflake-provider-normalization',
            ),
            pytest.param(
                SNOWFLAKE_CASE.connector_payload(
                    include_provider=False,
                    omit_fields=('warehouse',),
                    name='snowflake_wh',
                ),
                SNOWFLAKE_CASE.expected_connector_attrs(
                    name='snowflake_wh',
                    omit_fields=('warehouse',),
                )
                | {'type': DataConnectorType.DATABASE},
                id='infers-snowflake-provider',
            ),
        ],
    )
    def test_from_obj_normalizes_database_fields(
        self,
        payload: dict[str, object],
        expected: dict[str, object],
    ) -> None:
        """
        Test that :meth:`from_obj` preserves standard database connector
        fields.
        """
        connector = ConnectorDb.from_obj(payload)

        assert_connector_fields(connector, expected)

    @pytest.mark.parametrize(
        ('payload', 'expected_missing_fields'),
        [
            pytest.param(
                {
                    'provider': None,
                    'engine': 'gcp-bigquery',
                    'project': 'analytics-project',
                },
                ('dataset',),
                id='mapping-uses-engine-when-provider-empty',
            ),
            pytest.param(
                {
                    'provider': None,
                    'account': 'acme.us-east-1',
                    'database': 'analytics',
                },
                ('schema',),
                id='mapping-uses-hint-fields-when-provider-empty',
            ),
            pytest.param({}, (), id='missing-provider-returns-empty'),
            pytest.param(
                SimpleNamespace(provider='gcp-bigquery', project=None),
                ('project', 'dataset'),
                id='object-provider-fields',
            ),
        ],
    )
    def test_missing_provider_fields_infers_provider_for_raw_mapping(
        self,
        payload: dict[str, object],
        expected_missing_fields: tuple[str, ...],
    ) -> None:
        """Provider field detection should match raw mapping inference rules."""
        assert ConnectorDb.missing_provider_fields(payload) == expected_missing_fields

    @pytest.mark.parametrize(
        ('obj', 'provider', 'expected'),
        [
            pytest.param(
                {'project': 'analytics-project'},
                'sf',
                'snowflake',
                id='explicit-provider-argument-wins',
            ),
            pytest.param(
                SimpleNamespace(provider='gcp-bigquery'),
                None,
                'bigquery',
                id='object-provider-attribute-is-normalized',
            ),
        ],
    )
    def test_provider_from_value_covers_mapping_and_object_paths(
        self,
        obj: object,
        provider: str | None,
        expected: str,
    ) -> None:
        """Provider resolution should normalize both explicit and object hints."""
        assert ConnectorDb._provider_from_value(obj, provider=provider) == expected

    @pytest.mark.parametrize(
        ('provider', 'expected_name', 'expected_fields', 'expected_issue'),
        [
            pytest.param(
                'postgresql',
                'Postgres',
                (),
                None,
                id='alias-normalization-and-display-fallback',
            ),
            pytest.param(
                'bigquery',
                'BigQuery',
                ('project', 'dataset'),
                'missing connection_string or bigquery project/dataset',
                id='known-provider-metadata',
            ),
            pytest.param(
                None,
                None,
                (),
                None,
                id='missing-provider',
            ),
            pytest.param('   ', None, (), None, id='blank-provider'),
        ],
    )
    def test_provider_metadata_helpers_cover_supported_alias_and_none_paths(
        self,
        provider: str | None,
        expected_name: str | None,
        expected_fields: tuple[str, ...],
        expected_issue: str | None,
    ) -> None:
        """Provider helper methods should stay aligned on normalization rules."""
        assert ConnectorDb.provider_display_name(provider) == expected_name
        assert ConnectorDb.provider_required_fields(provider) == expected_fields
        assert ConnectorDb.provider_missing_connection_issue(provider) == expected_issue

    @pytest.mark.parametrize(
        ('provider', 'expected'),
        [
            pytest.param(
                'bigquery',
                (
                    'Set "connection_string" to a database DSN or SQLAlchemy-style '
                    'URL, or define both "project" and "dataset" for this '
                    'BigQuery connector.'
                ),
                id='two-required-fields',
            ),
            pytest.param(
                'snowflake',
                (
                    'Set "connection_string" to a database DSN or SQLAlchemy-style '
                    'URL, or define "account", "database", and "schema" for '
                    'this Snowflake connector.'
                ),
                id='three-required-fields',
            ),
            pytest.param(
                'postgres',
                None,
                id='no-provider-specific-fields',
            ),
            pytest.param(None, None, id='missing-provider'),
        ],
    )
    def test_provider_missing_connection_guidance_covers_supported_branches(
        self,
        provider: str | None,
        expected: str | None,
    ) -> None:
        """Guidance text should adapt to provider field-count requirements."""
        assert ConnectorDb.provider_missing_connection_guidance(provider) == expected

    def test_provider_missing_connection_guidance_supports_one_required_field(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Guidance formatting should also cover single-field provider rules."""
        monkeypatch.setitem(
            database_mod._DATABASE_PROVIDER_REQUIRED_FIELDS,
            'sqlite-local',
            ('file',),
        )

        assert ConnectorDb.provider_missing_connection_guidance('sqlite-local') == (
            'Set "connection_string" to a database DSN or SQLAlchemy-style URL, '
            'or define "file" for this Sqlite-Local connector.'
        )
