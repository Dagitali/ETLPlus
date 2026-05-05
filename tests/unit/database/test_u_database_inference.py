"""
:mod:`tests.unit.database.test_u_database_inference` module.

Unit tests for private database inference and value helpers.
"""

from __future__ import annotations

from datetime import date
from datetime import datetime
from datetime import time
from decimal import Decimal

import pytest

from etlplus.database._enums import DatabaseDialect
from etlplus.database._enums import SqlTypeAffinity
from etlplus.database._inferred_column import InferredColumn
from etlplus.database._schema_builder import SchemaBuilder
from etlplus.database._sql_dialect import SqlDialect
from etlplus.database._type_resolver import TypeResolver
from etlplus.database._value_codec import ValueCodec

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


class _Sentinel:
    """Object with deterministic string representation for JSON fallback."""

    def __str__(self) -> str:
        return 'sentinel'


# SECTION: TESTS ============================================================ #


class TestTypeResolver:
    """Unit tests for :class:`etlplus.database._type_resolver.TypeResolver`."""

    @pytest.mark.parametrize(
        ('py_types', 'expected'),
        [
            (set(), SqlTypeAffinity.TEXT),
            ({int, bool}, SqlTypeAffinity.INTEGER),
            ({int, float}, SqlTypeAffinity.REAL),
            ({float}, SqlTypeAffinity.REAL),
            ({Decimal}, SqlTypeAffinity.NUMERIC),
            ({bytes}, SqlTypeAffinity.BINARY),
            ({date}, SqlTypeAffinity.DATE),
            ({datetime}, SqlTypeAffinity.DATETIME),
            ({time}, SqlTypeAffinity.TIME),
            ({str, int}, SqlTypeAffinity.TEXT),
        ],
    )
    def test_resolve_returns_type_affinity(
        self,
        py_types: set[type],
        expected: SqlTypeAffinity,
    ) -> None:
        """Test that Python type sets resolve to portable affinities."""
        assert TypeResolver().resolve(py_types) is expected

    def test_resolve_arbitrary_mixed_types_can_prefer_numeric(self) -> None:
        """Test the non-default mixed-type fallback strategy."""
        resolver = TypeResolver(prefer_text_on_mixed=False)

        assert resolver.resolve({str, bytes}) is SqlTypeAffinity.NUMERIC


class TestSchemaBuilder:
    """Unit tests for :class:`etlplus.database._schema_builder.SchemaBuilder`."""

    def test_infer_columns_returns_inferred_columns(self) -> None:
        """Test that records produce ordered inferred columns."""
        columns = SchemaBuilder().infer_columns(
            [{'id': 1, 'name': 'Ada'}, {'id': 2, 'name': None}],
        )

        assert columns == [
            InferredColumn('id', SqlTypeAffinity.INTEGER, nullable=False),
            InferredColumn('name', SqlTypeAffinity.TEXT, nullable=True),
        ]
        assert columns[0].odbc_type == 'INTEGER'

    def test_infer_columns_honors_type_hints(self) -> None:
        """Test type hints override inferred affinities."""
        columns = SchemaBuilder().infer_columns(
            [{'payload': {'a': 1}}, {'payload': None}],
            type_hints={'payload': 'json'},
        )

        assert columns == [
            InferredColumn('payload', SqlTypeAffinity.JSON, nullable=True),
        ]


class TestSqlDialect:
    """Unit tests for :class:`etlplus.database._sql_dialect.SqlDialect`."""

    def test_quote_ident_escapes_embedded_quotes(self) -> None:
        """Test identifier quote escaping."""
        assert SqlDialect().quote_ident('bad"name') == '"bad""name"'

    def test_quote_ident_rejects_empty_identifier(self) -> None:
        """Test that blank identifiers are rejected before quoting."""
        with pytest.raises(ValueError, match='Invalid identifier'):
            SqlDialect().quote_ident('  ')

    @pytest.mark.parametrize(
        ('dialect', 'table_name', 'expected'),
        [
            (DatabaseDialect.SQLITE, 'dbo.Customers', '"Customers"'),
            (DatabaseDialect.SQLITE, '.Customers', '"Customers"'),
            (DatabaseDialect.SQLITE, 'main.Customers', '"main"."Customers"'),
            (
                DatabaseDialect.POSTGRESQL,
                'public.Customers',
                '"public"."Customers"',
            ),
            (DatabaseDialect.SQLITE, 'Customers', '"Customers"'),
        ],
    )
    def test_quote_table(
        self,
        dialect: DatabaseDialect,
        table_name: str,
        expected: str,
    ) -> None:
        """Test dialect-aware table quoting."""
        assert SqlDialect(dialect).quote_table(table_name) == expected

    def test_quote_table_rejects_empty_reference(self) -> None:
        """Test dotted empty table references are rejected."""
        for table_name, match in [
            ('...', 'Invalid table reference'),
            ('   ', 'Invalid identifier'),
        ]:
            with pytest.raises(ValueError, match=match):
                SqlDialect().quote_table(table_name)


class TestValueCodec:
    """Unit tests for :class:`etlplus.database._value_codec.ValueCodec`."""

    @pytest.mark.parametrize(
        ('value', 'sql_type', 'expected'),
        [
            (None, SqlTypeAffinity.TEXT, None),
            (True, SqlTypeAffinity.TEXT, 1),
            (5, SqlTypeAffinity.INTEGER, 5),
            ('42', 'int', 42),
            ('1e3', SqlTypeAffinity.INTEGER, 1000),
            (3.9, SqlTypeAffinity.INTEGER, 3),
            (float('nan'), SqlTypeAffinity.INTEGER, None),
            ('nan-ish', SqlTypeAffinity.INTEGER, None),
            (5, SqlTypeAffinity.REAL, 5.0),
            ('3.5', SqlTypeAffinity.REAL, 3.5),
            (float('inf'), SqlTypeAffinity.REAL, None),
            (Decimal('4.1'), SqlTypeAffinity.REAL, 4.1),
            (object(), SqlTypeAffinity.REAL, None),
            (7, SqlTypeAffinity.NUMERIC, '7'),
            (Decimal('3.10'), SqlTypeAffinity.NUMERIC, '3.10'),
            ('raw', SqlTypeAffinity.NUMERIC, 'raw'),
            (object(), SqlTypeAffinity.NUMERIC, None),
            ({'a': 1}, SqlTypeAffinity.BINARY, b'{"a": 1}'),
            (b'abc', SqlTypeAffinity.BINARY, b'abc'),
            ({'a': 1}, SqlTypeAffinity.JSON, '{"a": 1}'),
            ({'d': date(2026, 1, 2)}, SqlTypeAffinity.JSON, '{"d": "2026-01-02"}'),
            (
                {'amount': Decimal('1.2')},
                SqlTypeAffinity.JSON,
                '{"amount": "1.2"}',
            ),
            (_Sentinel(), SqlTypeAffinity.JSON, '"sentinel"'),
            (
                datetime(2026, 1, 2, 3, 4, 5, 6),
                SqlTypeAffinity.TEXT,
                '2026-01-02T03:04:05.000006',
            ),
            (date(2026, 1, 2), SqlTypeAffinity.TEXT, '2026-01-02'),
            (time(3, 4, 5, 6), SqlTypeAffinity.TEXT, '03:04:05.000006'),
        ],
    )
    def test_to_db_type_branches(
        self,
        value: object,
        sql_type: SqlTypeAffinity | str,
        expected: object,
    ) -> None:
        """Test target-specific value normalization branches."""
        assert ValueCodec().to_db(value, sql_type) == expected

    @pytest.mark.parametrize(
        ('keep_unknown_as_json', 'expected'),
        [
            (True, '{"a": 1}'),
            (False, "{'a': 1}"),
        ],
    )
    def test_text_fallback_respects_unknown_json_option(
        self,
        keep_unknown_as_json: bool,
        expected: str,
    ) -> None:
        """Test text fallback handling for complex values."""
        codec = ValueCodec(keep_unknown_as_json=keep_unknown_as_json)

        assert codec.to_db({'a': 1}, SqlTypeAffinity.TEXT) == expected
