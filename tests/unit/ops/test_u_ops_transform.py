"""
:mod:`tests.unit.ops.test_u_ops_transform` module.

Unit tests for :mod:`etlplus.ops.transform`.

Notes
-----
- Intentionally lightweight and pure (no network/filesystem beyond temporary
    JSON fixtures).
- Uses small in-memory datasets to validate each public operation and
    selected internal helpers.
- Covers public API, edge cases, and basic error-handling behavior.
- Validates stable behavior for edge cases (empty inputs, missing fields).
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Literal
from typing import cast

import pytest

# from etlplus.ops import transform as tx
from etlplus.ops._enums import AggregateName
from etlplus.ops._enums import OperatorName
from etlplus.ops._enums import PipelineStep
from etlplus.ops.transform import _normalize_operation_keys
from etlplus.ops.transform import _normalize_specs
from etlplus.ops.transform import apply_aggregate
from etlplus.ops.transform import apply_filter
from etlplus.ops.transform import apply_map
from etlplus.ops.transform import apply_select
from etlplus.ops.transform import apply_sort
from etlplus.ops.transform import transform
from etlplus.ops.transformations.aggregate import _agg_avg
from etlplus.ops.transformations.aggregate import _agg_count
from etlplus.ops.transformations.aggregate import _agg_max
from etlplus.ops.transformations.aggregate import _agg_min
from etlplus.ops.transformations.aggregate import _agg_sum
from etlplus.ops.transformations.aggregate import _collect_numeric_and_presence
from etlplus.ops.transformations.aggregate import _derive_agg_key
from etlplus.ops.transformations.aggregate import _resolve_aggregator
from etlplus.ops.transformations.aggregate import apply_aggregate_step
from etlplus.ops.transformations.filter import _contains
from etlplus.ops.transformations.filter import _eval_condition
from etlplus.ops.transformations.filter import _has
from etlplus.ops.transformations.filter import _resolve_operator
from etlplus.ops.transformations.filter import apply_filter_step
from etlplus.ops.transformations.map import apply_map_step
from etlplus.ops.transformations.select import apply_select_step
from etlplus.ops.transformations.select import is_plain_fields_list
from etlplus.ops.transformations.sort import _sort_key
from etlplus.ops.transformations.sort import apply_sort_step
from etlplus.utils._types import JSONData

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


transform_mod = importlib.import_module('etlplus.ops.transform')


type StepType = Literal['aggregate', 'filter', 'map', 'select', 'sort']


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='rows_people')
def rows_people_fixture() -> list[dict[str, Any]]:
    """Return a small dataset of people-like records."""

    return [
        {'name': 'John', 'age': 30, 'city': 'New York', 'status': 'active'},
        {'name': 'Jane', 'age': 25, 'city': 'Newark', 'status': 'inactive'},
        {'name': 'Bob', 'age': 35, 'city': 'Boston', 'status': 'active'},
    ]


@pytest.fixture(name='rows_values')
def rows_values_fixture() -> list[dict[str, Any]]:
    """Return a small dataset with a numeric ``value`` field."""

    return [
        {'name': 'John', 'value': 10},
        {'name': 'Jane', 'value': 20},
        {'name': 'Bob', 'value': 15},
    ]


# SECTION: TESTS ============================================================ #


class TestApplyAggregate:
    """Unit tests for :func:`apply_aggregate`."""

    @pytest.mark.parametrize(
        ('func', 'expected_key', 'expected_value'),
        [
            ('avg', 'avg_value', 15),
            ('count', 'count_value', 3),
            ('min', 'min_value', 10),
            ('max', 'max_value', 20),
            ('sum', 'sum_value', 45),
        ],
        ids=['avg', 'count', 'min', 'max', 'sum'],
    )
    def test_aggregate_builtin(
        self,
        rows_values: list[dict[str, int]],
        func: str,
        expected_key: str,
        expected_value: int,
    ) -> None:
        """Test aggregating ``value`` with built-in aggregator names."""
        result = apply_aggregate(
            rows_values,
            {'field': 'value', 'func': func},
        )
        assert result[expected_key] == expected_value

    def test_aggregate_callable_with_alias(
        self,
        rows_values: list[dict[str, int]],
    ) -> None:
        """Test aggregating with a callable and a custom alias."""

        def score(nums: list[float], present: int) -> float:
            return sum(nums) + present

        result = apply_aggregate(
            rows_values,
            {
                'field': 'value',
                'func': score,
                'alias': 'score',
            },
        )
        assert result == {'score': 48}


class TestApplyFilter:
    """Unit tests for :func:`apply_filter`."""

    def test_filter_basic_gte(self) -> None:
        """Test that filter keeps only records matching the predicate."""
        data = [{'age': 10}, {'age': 20}, {'age': 30}]
        result = apply_filter(
            data,
            {'field': 'age', 'op': 'gte', 'value': 20},
        )
        assert result == [{'age': 20}, {'age': 30}]

    @pytest.mark.parametrize(
        ('data', 'op', 'value', 'expected_names'),
        [
            (
                [
                    {'name': 'John'},
                    {'name': 'Jane'},
                    {'name': 'Bob'},
                ],
                lambda v, n: n in v.lower(),
                'a',
                ['Jane'],
            ),
        ],
    )
    def test_filter_callable_operator(
        self,
        data: list[dict[str, str]],
        op: Callable[[str, str], bool],
        value: str,
        expected_names: list[str],
    ) -> None:
        """Test that filtering supports custom callable operators."""
        result = apply_filter(
            data,
            {
                'field': 'name',
                'op': op,
                'value': value,
            },
        )
        assert [item['name'] for item in result] == expected_names

    def test_filter_empty_input(self) -> None:
        """Test that filtering an empty list returns an empty list."""

        result = apply_filter(
            [],
            {'field': 'age', 'op': 'gte', 'value': 10},
        )
        assert not result

    def test_filter_in_operator(
        self,
        rows_people: list[dict[str, Any]],
    ) -> None:
        """Test filtering with the ``in`` operator."""

        result = apply_filter(
            rows_people,
            {
                'field': 'status',
                'op': 'in',
                'value': ['active', 'pending'],
            },
        )
        assert [r['name'] for r in result] == ['John', 'Bob']

    def test_filter_missing_field_returns_empty(self) -> None:
        """
        Test filtering on a missing field returns an empty list.
        """
        data = [{'foo': 1}, {'foo': 2}]
        result = apply_filter(
            data,
            {'field': 'age', 'op': 'gte', 'value': 10},
        )
        assert not result

    @pytest.mark.parametrize(
        ('data', 'op', 'value', 'expected_count'),
        [
            (
                [{'name': 'John', 'age': 30}, {'name': 'Jane', 'age': 25}],
                'gt',
                26,
                1,
            ),
            (
                [
                    {'name': 'John', 'age': 30},
                    {'name': 'Jane', 'age': 25},
                    {'name': 'Bob', 'age': 30},
                ],
                'eq',
                30,
                2,
            ),
            (
                [
                    {'name': 'John', 'age': '30'},
                    {'name': 'Jane', 'age': '25'},
                ],
                'gt',
                26,
                1,
            ),
        ],
        ids=['gt-int', 'eq-int', 'gt-str'],
    )
    def test_filter_numeric_ops(
        self,
        data: list[dict[str, Any]],
        op: str,
        value: int,
        expected_count: int,
    ) -> None:
        """
        Test filtering with standard numeric comparisons.
        """
        result = apply_filter(
            data,
            {'field': 'age', 'op': op, 'value': value},
        )
        assert len(result) == expected_count

    def test_filter_invalid_operator_returns_input(self) -> None:
        """Test that unknown operators result in the original data."""
        data = [{'age': 30}]
        result = apply_filter(
            data,
            {
                'field': 'age',
                'op': cast(Any, object()),
                'value': 40,
            },
        )
        assert result == data


class TestApplyMap:
    """Unit tests for :func:`apply_map`."""

    def test_map_renames_fields(self) -> None:
        """Test mapping/renaming fields in each record."""
        data = [
            {'old_name': 'John', 'age': 30},
            {'old_name': 'Jane', 'age': 25},
        ]
        result = apply_map(data, {'old_name': 'new_name'})
        assert all('new_name' in item for item in result)
        assert all('old_name' not in item for item in result)
        assert result[0]['new_name'] == 'John'
        assert result[0]['age'] == 30

    def test_map_missing_source_key_is_noop(self) -> None:
        """
        Test that mapping does not add a destination key when the source is
        missing.
        """
        data = [{'foo': 1}]
        result = apply_map(data, {'bar': 'baz'})
        assert result == [{'foo': 1}]


class TestApplySelect:
    """Unit tests for :func:`apply_select`."""

    def test_select_subset_of_fields(self) -> None:
        """
        Test selecting a subset of fields from each record.
        """
        data = [
            {'name': 'John', 'age': 30, 'city': 'NYC'},
            {'name': 'Jane', 'age': 25, 'city': 'LA'},
        ]
        result = apply_select(data, ['name', 'age'])
        assert all(set(item) == {'name', 'age'} for item in result)

    def test_select_missing_fields_sets_none(self) -> None:
        """
        Test that selecting missing fields includes them with a ``None`` value.
        """
        data = [{'foo': 1}]
        result = apply_select(data, ['bar'])
        assert result == [{'bar': None}]


class TestApplySort:
    """Unit tests for :func:`apply_sort`."""

    @pytest.mark.parametrize(
        ('reverse', 'expected_sorted_ages'),
        [(False, [25, 30, 35]), (True, [35, 30, 25])],
        ids=['asc', 'desc'],
    )
    def test_sort_by_field(
        self,
        reverse: bool,
        expected_sorted_ages: list[int],
    ) -> None:
        """
        Test that :func:`apply_sort` supports ascending and descending order.
        """

        data = [
            {'name': 'John', 'age': 30},
            {'name': 'Jane', 'age': 25},
            {'name': 'Bob', 'age': 35},
        ]
        result = apply_sort(data, 'age', reverse=reverse)
        assert [item['age'] for item in result] == expected_sorted_ages

    def test_sort_by_string_field(self) -> None:
        """
        Test that :func:`apply_sort` works for string fields as well as numeric
        fields.
        """
        data = [{'name': 'Bob', 'age': 20}, {'name': 'Ada', 'age': 10}]
        result = apply_sort(data, 'name')
        assert result == [
            {'name': 'Ada', 'age': 10},
            {'name': 'Bob', 'age': 20},
        ]

    def test_sort_missing_field_is_noop(self) -> None:
        """
        Test that :func:`apply_sort` preserves the original order when sorting
        by a missing field.
        """
        data = [{'foo': 2}, {'foo': 1}]
        assert apply_sort(data, 'bar') == data

    def test_sort_without_field_is_noop(self) -> None:
        """
        Test that :func:`apply_sort` returns the original data when no field is
        specified.
        """
        data = [{'name': 'John'}]
        assert apply_sort(data, None) == data


class TestTransform:
    """Unit tests for :func:`transform`."""

    def test_aggregate_skips_empty_step_outputs(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that empty aggregate outputs do not mutate records."""
        monkeypatch.setattr(
            transform_mod,
            'apply_aggregate_step',
            lambda _records, _spec: [],
        )
        data = [{'value': 1}]
        assert (
            transform(
                data,
                {'aggregate': {'field': 'value', 'func': 'sum'}},
            )
            == data
        )

    def test_aggregate_with_invalid_spec_is_ignored(self) -> None:
        """Test that aggregate step is skipped when spec is not a mapping."""

        data = [{'value': 1}, {'value': 2}]
        result = transform(data, {'aggregate': ['not-a-mapping']})
        assert isinstance(result, list)
        assert result == data

    def test_from_json_string(self) -> None:
        """
        Test that aggregate step is skipped when spec is not a mapping.
        """
        json_str = '[{"name": "John", "age": 30}]'
        result = transform(json_str, {'select': ['name']})
        assert isinstance(result, list)
        assert len(result) == 1
        assert 'age' not in result[0]

    def test_from_file(
        self,
        temp_json_file: Callable[[JSONData], Path],
    ) -> None:
        """Test transforming from a JSON file."""
        temp_path = temp_json_file([{'name': 'John', 'age': 30}])
        result = transform(temp_path, {'select': ['name']})
        assert isinstance(result, list)
        assert len(result) == 1
        assert 'age' not in result[0]

    def test_no_operations(self) -> None:
        """
        Test that transforming without operations returns input unchanged.
        """
        data = [{'name': 'John'}]
        result = transform(data)
        assert result == data

    def test_non_list_payload_from_load_data(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that non-list payloads bypass record-wise operations."""
        monkeypatch.setattr(transform_mod, 'load_data', lambda _source: 7)
        assert transform('ignored', {'filter': {'field': 'a'}}) == 7

    def test_pipeline(self) -> None:
        """Test that transform applies operations in sequence."""
        data = [{'name': 'Ada', 'age': 10}, {'name': 'Bob', 'age': 20}]

        ops: dict[StepType, Any] = {
            'filter': {'field': 'age', 'op': 'gte', 'value': 15},
            'map': {'name': 'person'},
            'select': ['person', 'age'],
            'sort': {'field': 'age'},
        }

        result = transform(data, ops)
        assert result == [{'person': 'Bob', 'age': 20}]

    def test_single_dict_returns_single_dict(self) -> None:
        """
        Test that single-dict inputs remain dicts after list-based processing.
        """
        result = transform(
            {'name': 'Ada', 'age': 31},
            {'select': ['name']},
        )
        assert result == {'name': 'Ada'}

    def test_skips_empty_normalized_specs(self) -> None:
        """Test that empty step spec lists are ignored."""
        data = [{'a': 1}]
        assert transform(data, {'filter': []}) == data

    def test_skips_unknown_step_applier(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that missing appliers skip step execution safely."""
        monkeypatch.delitem(transform_mod._STEP_APPLIERS, 'filter')
        data = [{'a': 1}]
        assert (
            transform(data, {'filter': {'field': 'a', 'op': 'eq', 'value': 1}}) == data
        )

    def test_with_aggregate(self) -> None:
        """Test transforming using an aggregate operation."""
        data = [
            {'name': 'John', 'value': 10},
            {'name': 'Jane', 'value': 20},
        ]
        result = transform(
            data,
            {'aggregate': {'field': 'value', 'func': 'sum'}},
        )
        assert isinstance(result, dict)
        assert len(result) == 1
        assert result == {'sum_value': 30}

    def test_with_filter(self) -> None:
        """Test transforming using a filter operation."""
        data = [{'name': 'John', 'age': 30}, {'name': 'Jane', 'age': 25}]
        result = transform(
            data,
            {'filter': {'field': 'age', 'op': 'gt', 'value': 26}},
        )
        assert result == [{'name': 'John', 'age': 30}]

    def test_with_map(self) -> None:
        """Test transforming with a map operation."""
        data = [{'old_field': 'value'}]
        result = transform(data, {'map': {'old_field': 'new_field'}})
        assert isinstance(result, list)
        assert len(result) == 1
        assert result == [{'new_field': 'value'}]

    def test_with_multiple_aggregates(self) -> None:
        """
        Test transforming with multiple aggregation specs.
        """
        data = [{'value': 1}, {'value': 2}, {'value': 3}]
        result = transform(
            data,
            {
                'aggregate': [
                    {'field': 'value', 'func': 'sum'},
                    {'field': 'value', 'func': 'count', 'alias': 'count'},
                ],
            },
        )
        assert result == {'sum_value': 6, 'count': 3}

    def test_with_multiple_filters_and_select(
        self,
        rows_people: list[dict[str, Any]],
    ) -> None:
        """Test transforming with multiple filters and a select sequence."""

        def starts_with(value: object, prefix: str) -> bool:
            return str(value).startswith(prefix)

        result = transform(
            rows_people,
            {
                'filter': [
                    {'field': 'age', 'op': 'gte', 'value': 26},
                    {'field': 'city', 'op': starts_with, 'value': 'New'},
                ],
                'select': [{'fields': ['name']}],
            },
        )
        assert result == [{'name': 'John'}]

    def test_with_select(self) -> None:
        """Test transforming with a select operation."""
        data = [{'name': 'John', 'age': 30, 'city': 'NYC'}]
        result = transform(data, {'select': ['name', 'age']})
        assert isinstance(result, list)
        assert len(result) == 1
        assert result == [{'name': 'John', 'age': 30}]

    def test_with_sort(self) -> None:
        """Test that transforming with a sort operation sorts records."""

        data = [{'name': 'John', 'age': 30}, {'name': 'Jane', 'age': 25}]
        result = transform(data, {'sort': {'field': 'age'}})
        assert isinstance(result, list)
        assert len(result) == 2
        assert result == [
            {'name': 'Jane', 'age': 25},
            {'name': 'John', 'age': 30},
        ]


class TestTransformInternalHelpers:
    """Unit tests for internal helpers in :mod:`etlplus.ops.transform`."""

    @pytest.mark.parametrize(
        ('fn', 'nums', 'present', 'expected'),
        [
            (_agg_avg, [], 0, 0.0),
            (_agg_avg, [1, 2, 3], 3, 2.0),
            (_agg_count, [], 0, 0),
            (_agg_count, [1, 2, 3], 5, 5),
            (_agg_max, [], 0, None),
            (_agg_max, [1, 2, 3], 0, 3),
            (_agg_min, [], 0, None),
            (_agg_min, [1, 2, 3], 0, 1),
            (_agg_sum, [], 0, 0),
            (_agg_sum, [1, 2, 3], 0, 6),
        ],
        ids=[
            'avg-empty',
            'avg',
            'count-empty',
            'count',
            'max-empty',
            'max',
            'min-empty',
            'min',
            'sum-empty',
            'sum',
        ],
    )
    def test_agg_helpers(
        self,
        fn: Callable[[list[float], int], Any],
        nums: list[float],
        present: int,
        expected: Any,
    ) -> None:
        """Test that aggregator helpers return expected results."""
        result = fn(nums, present)
        if isinstance(expected, float):
            assert result == pytest.approx(expected)
        else:
            assert result == expected

    def test_apply_aggregate_invalid_operation_returns_error(self) -> None:
        """
        Test that aggregates missing field/func returns structured errors.
        """
        assert apply_aggregate([{'a': 1}], {'field': 'a'}) == {
            'error': 'Invalid aggregation operation',
        }

    def test_apply_aggregate_step(self) -> None:
        """
        Test that :func:`apply_aggregate_step` returns a correct aggregation.
        """
        rows = [{'a': 1}, {'a': 2}]
        spec = {'field': 'a', 'func': 'sum', 'alias': 'total'}
        result = apply_aggregate_step(rows, spec)
        assert result == [{'total': 3}]

    def test_apply_aggregate_step_non_mapping_is_noop(self) -> None:
        """Test that aggregate step ignores non-mapping specs."""
        rows = [{'a': 1}]

        assert apply_aggregate_step(rows, cast(Any, 'sum')) is rows

    def test_apply_aggregate_unknown_function_returns_error(self) -> None:
        """Test that unknown aggregate functions return structured errors."""
        result = apply_aggregate(
            [{'a': 1}],
            {'field': 'a', 'func': cast(Any, object())},
        )
        assert result['error'].startswith('Unknown aggregation function:')

    def test_apply_filter_returns_input_when_value_is_none(self) -> None:
        """
        Test that filters with ``None`` values are treated as invalid specs.
        """
        rows = [{'a': 1}]
        assert (
            apply_filter(
                rows,
                {'field': 'a', 'op': 'eq', 'value': None},
            )
            == rows
        )

    def test_apply_filter_skips_records_when_operator_raises_type_error(
        self,
    ) -> None:
        """
        Test that type errors during comparison skip only offending records.
        """

        def _explode(_a: Any, _b: Any) -> bool:
            raise TypeError('unsupported compare')

        rows = [{'name': 'Ada'}, {'name': 'Bob'}]
        assert not apply_filter(
            rows,
            {'field': 'name', 'op': _explode, 'value': 'A'},
        )

    def test_apply_filter_step(self) -> None:
        """
        Test that :func:`apply_filter_step` returns correct filtered rows.
        """
        rows = [{'a': 1}, {'a': 2}]
        spec = {'field': 'a', 'op': 'gt', 'value': 1}
        result = apply_filter_step(rows, spec)
        assert result == [{'a': 2}]

    @pytest.mark.parametrize(
        'spec',
        [
            pytest.param('age', id='non-mapping'),
            pytest.param({'field': 'a', 'value': 1}, id='missing-op'),
            pytest.param({'field': 'a', 'op': object(), 'value': 1}, id='bad-op'),
        ],
    )
    def test_apply_filter_step_invalid_specs_are_noop(self, spec: Any) -> None:
        """Test that invalid filter step specs leave rows unchanged."""
        rows = [{'a': 1}]

        assert apply_filter_step(rows, spec) is rows

    def test_apply_filter_step_returns_input_when_field_missing(self) -> None:
        """
        Test that filter step returns input unchanged when field is absent.
        """
        rows = [{'a': 1}]
        assert apply_filter_step(rows, {'op': 'eq', 'value': 1}) == rows

    def test_apply_map_step(self) -> None:
        """
        Test that :func:`apply_map_step` returns correct mapped records.
        """
        rows = [{'a': 1, 'b': 2}]
        spec = {'a': 'x', 'b': 'y'}
        result = apply_map_step(rows, spec)
        assert result == [{'x': 1, 'y': 2}]

    def test_apply_map_step_non_mapping_is_noop(self) -> None:
        """Test that map step is a no-op for non-mapping specs."""
        rows = [{'a': 1}]
        assert apply_map_step(rows, 123) == rows

    def test_apply_row_step_returns_original_rows_for_empty_specs(self) -> None:
        """Test that empty normalized specs leave row batches unchanged."""
        rows = [{'id': 1}]

        assert (
            transform_mod._apply_row_step(
                rows,
                step='filter',
                raw_spec=[],
            )
            is rows
        )

    @pytest.mark.parametrize(
        ('spec', 'expected'),
        [
            ({'fields': ['a']}, [{'a': 1}]),
            (['a'], [{'a': 1}]),
            (123, [{'a': 1, 'b': 2}]),
        ],
        ids=['mapping', 'list', 'other'],
    )
    def test_apply_select_step(
        self,
        spec: object,
        expected: list[dict[str, int]],
    ) -> None:
        """
        Test that :func:`apply_select_step` returns correct selected fields
        for a given spec.
        """
        rows = [{'a': 1, 'b': 2}]
        result = apply_select_step(rows, spec)
        assert result == expected

    def test_apply_select_step_invalid_mapping_fields_is_noop(self) -> None:
        """
        Test that :func:`apply_select_step` is a no-op when mapping fields
        value is invalid.
        """
        rows = [{'a': 1, 'b': 2}]
        assert apply_select_step(rows, {'fields': {'not': 'list'}}) == rows

    @pytest.mark.parametrize(
        ('spec', 'expected'),
        [
            ({'field': 'a'}, [{'a': 1}, {'a': 2}]),
            (None, [{'a': 2}, {'a': 1}]),
            ('a', [{'a': 1}, {'a': 2}]),
        ],
        ids=['mapping', 'none', 'other'],
    )
    def test_apply_sort_step(
        self,
        spec: object | None,
        expected: list[dict[str, int]],
    ) -> None:
        """
        Test that :func:`apply_sort_step` returns correct sorted records for a
        given spec.
        """
        rows = [{'a': 2}, {'a': 1}]
        result = apply_sort_step(rows, spec)
        assert result == expected

    def test_collect_numeric_and_presence(self) -> None:
        """
        Test that :func:`_collect_numeric_and_presence` returns correct numeric
        values and their count.
        """
        rows = [{'a': 1}, {'a': 2}, {'b': 3}]
        nums, present = _collect_numeric_and_presence(rows, None)
        assert not nums
        assert present == 3

        nums, present = _collect_numeric_and_presence(rows, 'a')
        assert nums == [1, 2]
        assert present == 2

    def test_collect_numeric_and_presence_ignores_non_numeric_values(
        self,
    ) -> None:
        """
        Test that presence increments even when values are non-numeric.
        """
        rows: list[dict[str, Any]] = [{'a': 'x'}, {'a': True}, {'a': 2}]
        nums, present = _collect_numeric_and_presence(rows, 'a')
        assert nums == [2.0]
        assert present == 3

    def test_contains(self) -> None:
        """
        Test that :func:`_contains` returns correct truthy values.
        """
        assert _contains([1, 2, 3], 2)
        assert not _contains([1, 2, 3], 5)

    def test_contains_typeerror(self) -> None:
        """
        Test that :func:`_contains` handles :class:`TypeError` gracefully.
        """

        class NoContains:
            """Type that is not iterable and does not implement containment."""

        assert not _contains(NoContains(), 1)

    @pytest.mark.parametrize(
        ('func', 'field', 'alias', 'expected'),
        [
            ('sum', 'foo', 'total', 'total'),
            (AggregateName.SUM, 'foo', None, 'sum_foo'),
            ('sum', 'foo', None, 'sum_foo'),
        ],
        ids=['alias', 'enum', 'string'],
    )
    def test_derive_agg_key_common(
        self,
        func: AggregateName | str,
        field: str,
        alias: str | None,
        expected: str,
    ) -> None:
        """
        Test that :func:`_derive_agg_key` derives stable keys for common cases.
        """
        assert _derive_agg_key(func, field, alias) == expected

    def test_derive_agg_key_callable(self) -> None:
        """
        Test that :func:`_derive_agg_key` handles callable aggregators.
        """

        def agg(xs: list[float], n: int) -> float:
            return 0.0

        assert _derive_agg_key(agg, 'foo', None).startswith('agg_')

    def test_derive_agg_key_other(self) -> None:
        """
        Test that :func:`_derive_agg_key` handles unknown object inputs
        consistently.
        """

        class Dummy:
            """Dummy class for testing."""

        val = _derive_agg_key(cast(Any, Dummy()), 'foo', None)
        assert val.endswith('_foo')

    @pytest.mark.parametrize(
        ('strict', 'should_raise'),
        [(True, False), (False, True)],
        ids=['strict', 'non-strict'],
    )
    def test_eval_condition_exception_behavior(
        self,
        strict: bool,
        should_raise: bool,
    ) -> None:
        """
        Test that :func:`_eval_condition` optionally re- raises operator
        errors.
        """
        rec = {'a': 2}

        def op(_: object, __: object) -> bool:
            raise RuntimeError('fail')

        if should_raise:
            with pytest.raises(RuntimeError):
                _eval_condition(rec, 'a', op, 2, strict)
        else:
            assert not _eval_condition(rec, 'a', op, 2, strict)

    def test_eval_condition_truth_table(self) -> None:
        """
        Test that :func:`_eval_condition` evaluates comparisons correctly.
        """
        rec = {'a': 2}

        def eq(a: object, b: object) -> bool:
            return a == b

        assert _eval_condition(rec, 'a', eq, 2, True)
        assert not _eval_condition(rec, 'a', eq, 3, True)
        assert not _eval_condition({'b': 2}, 'a', eq, 2, True)

    def test_has(self) -> None:
        """Test that :func:`_has` returns correct truthy value."""
        assert _has(2, [1, 2, 3])
        assert not _has(5, [1, 2, 3])

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            (['name', 'age'], True),
            (('city',), True),
            (['name', {'nested': 'no'}], False),
            ('name', False),
        ],
    )
    def test_is_plain_fields_list(
        self,
        value: object,
        expected: bool,
    ) -> None:
        """
        Test that :func:`is_plain_fields_list` returns ``True`` only for plain
        sequences of non-mappings.
        """
        assert is_plain_fields_list(value) is expected

    def test_normalize_operation_keys_accepts_enums(self) -> None:
        """
        Test that :class:`PipelineStep` keys normalize to lowercase strings.
        """
        operations = {
            PipelineStep.FILTER: {'field': 'age', 'op': 'gt', 'value': 20},
            'map': {'old': 'new'},
        }

        normalized = _normalize_operation_keys(operations)
        assert set(normalized) == {'filter', 'map'}
        assert normalized['filter']['field'] == 'age'

    def test_normalize_operation_keys_fallback_paths(self) -> None:
        """
        Test that fallback key normalization handles `.value` and skip non-str.
        """

        class _WithValue:
            value = 'custom'

        class _WithNonStringValue:
            value = 123

        normalized = _normalize_operation_keys(
            {
                _WithValue(): {'x': 1},
                _WithNonStringValue(): {'y': 2},
            },
        )
        assert normalized == {'custom': {'x': 1}}

    def test_normalize_operation_keys_pipeline_step_isinstance_branch(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that pipeline-step instance branch uses `.value` as key."""

        @dataclass(slots=True, frozen=True)
        class _Step:
            value: str

        monkeypatch.setattr(transform_mod, 'PipelineStep', _Step)

        normalized = _normalize_operation_keys({_Step('filter'): {'x': 1}})
        assert normalized == {'filter': {'x': 1}}

    def test_normalize_specs_handles_scalar_and_sequence(self) -> None:
        """
        Test that :func:`_normalize_specs` coerces scalars to list and keeps
        sequences.
        """

        single = {'field': 'age'}
        assert not _normalize_specs(None)
        assert _normalize_specs(single) == [single]
        assert _normalize_specs([single, single]) == [single, single]

    def test_resolve_aggregator(self) -> None:
        """
        Test that :func:`_resolve_aggregator` accepts enums, strings, and
        callables.
        """
        fn = _resolve_aggregator(AggregateName.SUM)
        assert callable(fn)
        assert fn([1, 2], 2) == 3

        fn = _resolve_aggregator('avg')
        assert callable(fn)
        assert fn([2, 4], 2) == 3

        def agg(xs: list[float], n: int) -> float:
            return 42.0

        assert _resolve_aggregator(agg) is agg

        with pytest.raises(TypeError):
            _resolve_aggregator(cast(Any, object()))

    def test_resolve_operator(self) -> None:
        """
        Test that :func:`_resolve_operator` accepts enums, strings, and
        callables.
        """
        fn = _resolve_operator(OperatorName.EQ)
        assert fn(1, 1)
        assert not fn(1, 2)

        fn = _resolve_operator('gt')
        assert fn(2, 1)
        assert not fn(1, 2)

        def op(a: object, b: object) -> bool:
            return a == b

        assert _resolve_operator(op) is op

        with pytest.raises(TypeError):
            _resolve_operator(cast(Any, object()))

    def test_resolve_operator_falls_back_to_raw_values_when_non_numeric(
        self,
    ) -> None:
        """
        Test that numeric wrappers fall back to base operators for strings.
        """
        fn = _resolve_operator('gt')
        assert fn('b', 'a') is True

    def test_sort_key(self) -> None:
        """
        Test that :func:`_sort_key` places numbers before strings, then `None`
        values last.
        """
        assert _sort_key(None)[0] == 2
        assert _sort_key(5)[0] == 0
        assert _sort_key(True)[0] == 1
        assert _sort_key('abc')[0] == 1
