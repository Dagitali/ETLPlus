"""
:mod:`tests.unit.file.test_u_file_r` module.

Unit tests for :mod:`etlplus.file._r`.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from etlplus.file import _r as mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: INTERNAL CLASSES ================================================= #


@dataclass
class _DataFrameStub:
    """Minimal DataFrame-like stub for :mod:`etlplus.file._r` tests."""

    records: list[dict[str, object]]

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:
        """Return row records for ``orient='records'``."""
        assert orient == 'records'
        return list(self.records)


# SECTION: TESTS ============================================================ #


class TestRHelpers:
    """Unit tests for R result coercion helpers."""

    @pytest.fixture(name='pandas_stub')
    def pandas_stub_fixture(self) -> SimpleNamespace:
        """Return one pandas-like namespace exposing the frame stub."""
        return SimpleNamespace(DataFrame=_DataFrameStub)

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            (_DataFrameStub([{'id': 1}]), [{'id': 1}]),
            ({'a': 1}, {'a': 1}),
            ([{'a': 1}], [{'a': 1}]),
            ([1, 2], {'value': [1, 2]}),
            ('x', {'value': 'x'}),
        ],
    )
    def test_coerce_r_object(
        self,
        pandas_stub: SimpleNamespace,
        value: object,
        expected: object,
    ) -> None:
        """Test R object coercion for supported object categories."""
        assert mod.coerce_r_object(value, pandas_stub) == expected

    @pytest.mark.parametrize('dataset', ['only', 'data'])
    def test_coerce_r_result_dataset_selection_and_alias(
        self,
        pandas_stub: SimpleNamespace,
        dataset: str,
    ) -> None:
        """Test explicit and alias dataset key resolution."""
        result: dict[str, object] = {'only': _DataFrameStub([{'id': 1}])}

        assert mod.coerce_r_result(
            result,
            dataset=dataset,
            dataset_key='data',
            format_name='RDS',
            pandas=pandas_stub,
        ) == [{'id': 1}]

    def test_coerce_r_result_empty_returns_empty_list(
        self,
        pandas_stub: SimpleNamespace,
    ) -> None:
        """Test empty R result normalization."""
        assert (
            mod.coerce_r_result(
                {},
                dataset=None,
                dataset_key='data',
                format_name='RDS',
                pandas=pandas_stub,
            )
            == []
        )

    def test_coerce_r_result_rejects_unknown_dataset(
        self,
        pandas_stub: SimpleNamespace,
    ) -> None:
        """Test missing explicit dataset selection."""
        with pytest.raises(
            ValueError,
            match="RDA dataset 'missing' not found",
        ):
            mod.coerce_r_result(
                {'known': {'a': 1}},
                dataset='missing',
                dataset_key='data',
                format_name='RDA',
                pandas=pandas_stub,
            )

    def test_coerce_r_result_without_dataset(
        self,
        pandas_stub: SimpleNamespace,
    ) -> None:
        """Test implicit dataset behavior for one vs many R objects."""
        single = mod.coerce_r_result(
            {'only': {'a': 1}},
            dataset=None,
            dataset_key='data',
            format_name='RDS',
            pandas=pandas_stub,
        )
        assert single == {'a': 1}

        many = mod.coerce_r_result(
            {'a': {'x': 1}, 'b': [1, 2, 3]},
            dataset=None,
            dataset_key='data',
            format_name='RDA',
            pandas=pandas_stub,
        )
        assert many == {'a': {'x': 1}, 'b': {'value': [1, 2, 3]}}

    def test_list_r_dataset_keys(self) -> None:
        """Test dataset key listing behavior."""
        assert mod.list_r_dataset_keys({}, default_key='data') == ['data']
        assert mod.list_r_dataset_keys(
            {'a': object(), '2': object()},
            default_key='x',
        ) == ['a', '2']
