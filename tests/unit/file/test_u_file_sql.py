"""
:mod:`tests.unit.file.test_u_file_sql` module.

Unit tests for :mod:`etlplus.file._sql`.
"""

from __future__ import annotations

import pytest

from etlplus.file import _sql as mod

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
        """Test collection of sorted column names and aligned values."""
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
        """Test SQL identifier quoting and escaping."""
        assert mod.quote_identifier('table') == '"table"'
        assert mod.quote_identifier('a"b') == '"a""b"'

    def test_resolve_table(self) -> None:
        """Test table resolution behavior for supported branches."""
        assert mod.resolve_table([], engine_name='sqlite') is None
        assert (
            mod.resolve_table(['data', 'other'], engine_name='sqlite')
            == 'data'
        )
        assert mod.resolve_table(['single'], engine_name='sqlite') == 'single'
        with pytest.raises(
            ValueError,
            match='Multiple tables found in sqlite file',
        ):
            mod.resolve_table(
                ['a', 'b'],
                engine_name='sqlite',
                default_table='data',
            )
