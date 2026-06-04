"""
:mod:`tests.unit.file.test_u_file_sql` module.

Unit tests for :mod:`etlplus.file._sql`.
"""

from __future__ import annotations

import pytest

from etlplus.file import _sql as mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestSqlHelpers:
    """Unit tests for shared SQL helper functions."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            (None, None),
            ('abc', 'abc'),
            (1, 1),
            (1.5, 1.5),
            (True, True),
            ({'a': 1}, '{"a": 1}'),
            ([1, 2], '[1, 2]'),
        ],
    )
    def test_coerce_sql_value(
        self,
        value: object,
        expected: object,
    ) -> None:
        """Test coercion of SQL-bound values."""
        assert mod.coerce_sql_value(value) == expected

    def test_collect_column_values(self) -> None:
        """Test that collection of sorted column names and aligned values."""
        columns, values = mod.collect_column_values(
            [
                {'b': 2},
                {'a': 1, 'b': 3},
            ],
        )
        assert columns == ['a', 'b']
        assert values == {'a': [None, 1], 'b': [2, 3]}

    @pytest.mark.parametrize(
        ('values', 'expected'),
        [
            ([None, None], mod.SQLITE_DIALECT.text),
            ([True, False], mod.SQLITE_DIALECT.boolean),
            ([1, 2], mod.SQLITE_DIALECT.integer),
            ([1, 2.5], mod.SQLITE_DIALECT.floating),
            (['x', 1], mod.SQLITE_DIALECT.text),
        ],
    )
    def test_infer_column_type(
        self,
        values: list[object],
        expected: str,
    ) -> None:
        """Test inferred SQL type precedence."""
        assert mod.infer_column_type(values, mod.SQLITE_DIALECT) == expected

    def test_quote_identifier_escapes_double_quotes(self) -> None:
        """Test that :func:`quote_identifier` handles quoting and escaping."""
        assert mod.quote_identifier('table') == '"table"'
        assert mod.quote_identifier('a"b') == '"a""b"'

    @pytest.mark.parametrize(
        ('tables', 'expected'),
        [
            ([], None),
            (['data', 'other'], 'data'),
            (['single'], 'single'),
        ],
    )
    def test_resolve_table_success_cases(
        self,
        tables: list[str],
        expected: str | None,
    ) -> None:
        """Test table resolution behavior for supported branches."""
        assert mod.resolve_table(tables, engine_name='sqlite') == expected

    def test_resolve_table_rejects_ambiguous_tables(self) -> None:
        """Test that ambiguous table sets raise when no default is present."""
        with pytest.raises(
            ValueError,
            match='Multiple tables found in sqlite file',
        ):
            mod.resolve_table(
                ['a', 'b'],
                engine_name='sqlite',
                default_table='data',
            )

    def test_write_table_rows_returns_zero_for_empty_rows(self) -> None:
        """
        Test that :func:`write_table_rows` short-circuit when no rows are
        provided.
        """
        assert (
            mod.write_table_rows(
                object(),
                'data',
                [],
                dialect=mod.SQLITE_DIALECT,
            )
            == 0
        )
