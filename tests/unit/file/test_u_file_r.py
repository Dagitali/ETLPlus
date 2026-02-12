"""
:mod:`tests.unit.file.test_u_file_r` module.

Unit tests for :mod:`etlplus.file._r`.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from etlplus.file import _r as mod

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

    def test_coerce_r_object_for_dataframe_dict_list_and_scalar(self) -> None:
        """Test R object coercion for supported object categories."""
        pandas = SimpleNamespace(DataFrame=_DataFrameStub)
        frame = _DataFrameStub([{'id': 1}])
        assert mod.coerce_r_object(frame, pandas) == [{'id': 1}]
        assert mod.coerce_r_object({'a': 1}, pandas) == {'a': 1}
        assert mod.coerce_r_object([{'a': 1}], pandas) == [{'a': 1}]
        assert mod.coerce_r_object([1, 2], pandas) == {'value': [1, 2]}
        assert mod.coerce_r_object('x', pandas) == {'value': 'x'}

    def test_coerce_r_result_dataset_selection_and_alias(self) -> None:
        """Test explicit and alias dataset key resolution."""
        pandas = SimpleNamespace(DataFrame=_DataFrameStub)
        result: dict[str, object] = {'only': _DataFrameStub([{'id': 1}])}

        selected = mod.coerce_r_result(
            result,
            dataset='only',
            dataset_key='data',
            format_name='RDS',
            pandas=pandas,
        )
        assert selected == [{'id': 1}]

        aliased = mod.coerce_r_result(
            result,
            dataset='data',
            dataset_key='data',
            format_name='RDS',
            pandas=pandas,
        )
        assert aliased == [{'id': 1}]

    def test_coerce_r_result_empty_returns_empty_list(self) -> None:
        """Test empty R result normalization."""
        pandas = SimpleNamespace(DataFrame=_DataFrameStub)
        assert (
            mod.coerce_r_result(
                {},
                dataset=None,
                dataset_key='data',
                format_name='RDS',
                pandas=pandas,
            )
            == []
        )

    def test_coerce_r_result_rejects_unknown_dataset(self) -> None:
        """Test missing explicit dataset selection."""
        pandas = SimpleNamespace(DataFrame=_DataFrameStub)
        with pytest.raises(
            ValueError,
            match="RDA dataset 'missing' not found",
        ):
            mod.coerce_r_result(
                {'known': {'a': 1}},
                dataset='missing',
                dataset_key='data',
                format_name='RDA',
                pandas=pandas,
            )

    def test_coerce_r_result_without_dataset(
        self,
    ) -> None:
        """Test implicit dataset behavior for one vs many R objects."""
        pandas = SimpleNamespace(DataFrame=_DataFrameStub)

        single = mod.coerce_r_result(
            {'only': {'a': 1}},
            dataset=None,
            dataset_key='data',
            format_name='RDS',
            pandas=pandas,
        )
        assert single == {'a': 1}

        many = mod.coerce_r_result(
            {'a': {'x': 1}, 'b': [1, 2, 3]},
            dataset=None,
            dataset_key='data',
            format_name='RDA',
            pandas=pandas,
        )
        assert many == {'a': {'x': 1}, 'b': {'value': [1, 2, 3]}}

    def test_list_r_dataset_keys(self) -> None:
        """Test dataset key listing behavior."""
        assert mod.list_r_dataset_keys({}, default_key='data') == ['data']
        assert mod.list_r_dataset_keys(
            {'a': object(), '2': object()},
            default_key='x',
        ) == ['a', '2']
