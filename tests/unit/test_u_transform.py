"""
:mod:`tests.unit.test_u_transform` module.

Unit tests for :mod:`etlplus.transform`.

Notes
-----
- Uses small in-memory datasets to validate each operation.
- Covers public API, edge cases, and basic error-handling behavior.
- Ensures stable behavior for edge cases (empty inputs, missing fields).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from typing import Literal

import pytest

from etlplus.enums import PipelineStep
from etlplus.transform import _is_plain_fields_list
from etlplus.transform import _normalize_operation_keys
from etlplus.transform import _normalize_specs
from etlplus.transform import apply_aggregate
from etlplus.transform import apply_filter
from etlplus.transform import apply_map
from etlplus.transform import apply_select
from etlplus.transform import apply_sort
from etlplus.transform import transform

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


type StepType = Literal['aggregate', 'filter', 'map', 'select', 'sort']


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestApplyAggregate:
    """Unit test suite for :func:`etlplus.transform.apply_aggregate`."""

    @pytest.mark.parametrize(
        'func, expected_result',
        [
            ('avg', 15),
            ('count', 3),
            ('min', 10),
            ('max', 20),
            ('sum', 45),
        ],
    )
    def test_aggregate(
        self,
        func: str,
        expected_result: int,
    ) -> None:
        """
        Test aggregating the ``value`` field with built-in functions.

        Parameters
        ----------
        func : str
            Aggregation function to apply.
        expected_result : int
            Expected result of the aggregation.
        """
        data = [
            {'name': 'John', 'value': 10},
            {'name': 'Jane', 'value': 20},
            {'name': 'Bob', 'value': 15},
        ]
        result = apply_aggregate(data, {'field': 'value', 'func': func})
        key = f'{func}_value' if func != 'count' else 'count_value'
        assert result[key] == expected_result

    def test_aggregate_callable_with_alias(self) -> None:
        """Test aggregating with a callable and custom alias."""

        def score(nums: list[float], present: int) -> float:
            return sum(nums) + present

        data = [
            {'value': 10},
            {'value': 20},
            {'value': 15},
        ]
        result = apply_aggregate(
            data,
            {
                'field': 'value',
                'func': score,
                'alias': 'score',
            },
        )
        assert result == {'score': 48}


@pytest.mark.unit
class TestApplyFilter:
    """Unit test suite for :func:`etlplus.transform.apply_filter`."""

    def test_filter_basic_gte(self) -> None:
        """Filter should keep only records matching the predicate."""

        data = [
            {'age': 10},
            {'age': 20},
            {'age': 30},
        ]

        result = apply_filter(
            data,
            {
                'field': 'age',
                'op': 'gte',
                'value': 20,
            },
        )

        assert result == [{'age': 20}, {'age': 30}]

    @pytest.mark.parametrize(
        'data, op, value, expected_names',
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
        """
        Test filtering with a custom callable operator.

        Parameters
        ----------
        data : list[dict[str, str]]
            Input records.
        op : Callable[[str, str], bool]
            Operator function.
        value : str
            Value to filter by.
        expected_names : list[str]
            Expected names after filter.
        """
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
            {
                'field': 'age',
                'op': 'gte',
                'value': 10,
            },
        )

        assert not result

    def test_filter_in(self) -> None:
        """
        Test filtering with the ``in`` operator.

        Notes
        -----
        Keeps records whose ``status`` is in the provided list.
        """
        data = [
            {'name': 'John', 'status': 'active'},
            {'name': 'Jane', 'status': 'inactive'},
            {'name': 'Bob', 'status': 'active'},
        ]
        result = apply_filter(
            data,
            {
                'field': 'status',
                'op': 'in',
                'value': ['active', 'pending'],
            },
        )
        assert len(result) == 2

    def test_filter_missing_field_returns_empty(self) -> None:
        """Test filtering on a missing field should return an empty list."""

        data = [{'foo': 1}, {'foo': 2}]

        result = apply_filter(
            data,
            {
                'field': 'age',
                'op': 'gte',
                'value': 10,
            },
        )

        assert not result

    @pytest.mark.parametrize(
        'data, op, value, expected_count',
        [
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
                    {'name': 'John', 'age': 30},
                    {'name': 'Jane', 'age': 25},
                    {'name': 'Bob', 'age': 35},
                ],
                'gt',
                28,
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
    )
    def test_filter_numeric_ops(
        self,
        data: list[dict],
        op: str,
        value: int | str,
        expected_count: int,
    ) -> None:
        """
        Test filtering with numeric operators.

        Parameters
        ----------
        data : list[dict]
            Input records.
        op : str
            Operator name.
        value : int | str
            Value to filter by.
        expected_count : int
            Expected number of filtered records.
        """
        result = apply_filter(data, {'field': 'age', 'op': op, 'value': value})
        assert len(result) == expected_count

    def test_filter_with_invalid_operator_returns_input(self) -> None:
        """Test that unknown operators results in the original data."""

        data = [{'age': 30}]
        result = apply_filter(
            data,
            {
                'field': 'age',
                'op': object(),
                'value': 40,
            },
        )

        assert result == data


@pytest.mark.unit
class TestApplyMap:
    """Unit test suite for :func:`etlplus.transform.apply_map`."""

    def test_map(self) -> None:
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


@pytest.mark.unit
class TestApplySelect:
    """Unit test suite for :func:`etlplus.transform.apply_select`."""

    def test_select(self) -> None:
        """
        Test selecting a subset of fields from each record.

        Notes
        -----
        Retains only ``name`` and ``age``.
        """
        data = [
            {'name': 'John', 'age': 30, 'city': 'NYC'},
            {'name': 'Jane', 'age': 25, 'city': 'LA'},
        ]
        result = apply_select(data, ['name', 'age'])
        assert all(set(item.keys()) == {'name', 'age'} for item in result)

    def test_select_missing_fields_sets_none(self) -> None:
        """
        Selecting missing fields should include them with a ``None`` value.
        """

        data = [{'foo': 1}]
        result = apply_select(data, ['bar'])

        assert result == [{'bar': None}]


@pytest.mark.unit
class TestApplySort:
    """Unit test suite for :func:`etlplus.transform.apply_sort`."""

    @pytest.mark.parametrize(
        'reverse, expected_sorted_ages',
        [
            (False, [25, 30, 35]),
            (True, [35, 30, 25]),
        ],
    )
    def test_sort(
        self,
        reverse: bool,
        expected_sorted_ages: list[int],
    ) -> None:
        """
        Test sorting records by a field.

        Parameters
        ----------
        reverse : bool
            Whether to sort in descending order.
        expected_sorted_ages : list[int]
            Expected sorted ages.

        Notes
        -----
        Checks ascending and descending sort by ``age``.
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
        Test that sorting works for string fields as well as numeric fields.
        """

        data = [
            {'name': 'Bob', 'age': 20},
            {'name': 'Ada', 'age': 10},
        ]

        result = apply_sort(data, 'name')

        assert result == [
            {'name': 'Ada', 'age': 10},
            {'name': 'Bob', 'age': 20},
        ]

    def test_sort_missing_field_is_noop(self) -> None:
        """
        Test that sorting by a missing field returns the original ordering.
        """

        data = [{'foo': 2}, {'foo': 1}]
        result = apply_sort(data, 'bar')

        assert result == [{'foo': 2}, {'foo': 1}]

    def test_sort_without_field_is_noop(self) -> None:
        """Test sorting without a field should return the original data."""

        data = [{'name': 'John'}]
        assert apply_sort(data, None) == data


@pytest.mark.unit
class TestTransform:
    """Unit test suite for :func:`etlplus.transform.transform`."""

    def test_aggregate_with_invalid_spec_is_ignored(self) -> None:
        """Test aggregate step should be skipped when spec is not a mapping."""

        data = [{'value': 1}, {'value': 2}]
        result = transform(data, {'aggregate': ['not-a-mapping']})
        assert isinstance(result, list)
        assert result == data

    def test_from_json_string(self) -> None:
        """
        Test transforming from a JSON string.

        Notes
        -----
        Selects only ``name`` from the provided JSON array string.
        """
        json_str = '[{"name": "John", "age": 30}]'
        result = transform(json_str, {'select': ['name']})
        assert len(result) == 1
        assert 'age' not in result

    def test_from_file(
        self,
        temp_json_file: Callable[[list[dict]], str],
    ) -> None:
        """
        Test transforming from a JSON file.

        Parameters
        ----------
        temp_json_file : Callable[[list[dict]], str]
            Fixture to create a temp JSON file.

        Notes
        -----
        Writes a temporary JSON file and selects only ``name``.
        """
        temp_path = temp_json_file([{'name': 'John', 'age': 30}])
        result = transform(temp_path, {'select': ['name']})
        assert len(result) == 1
        assert 'age' not in result

    def test_no_operations(self) -> None:
        """Test transforming without operations returns input unchanged."""
        data = [{'name': 'John'}]
        result = transform(data)
        assert result == data

    def test_with_aggregate(self) -> None:
        """
        Test transforming using an aggregate operation.

        Notes
        -----
        Sums the ``value`` field across records.
        """
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
        assert result['sum_value'] == 30

    def test_with_filter(self) -> None:
        """
        Test transforming using a filter operation.

        Notes
        -----
        Filters for ``age > 26``.
        """
        data = [
            {'name': 'John', 'age': 30},
            {'name': 'Jane', 'age': 25},
        ]
        result = transform(
            data,
            {
                'filter': {
                    'field': 'age',
                    'op': 'gt',
                    'value': 26,
                },
            },
        )
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]['name'] == 'John'

    def test_with_map(self) -> None:
        """
        Test transforming using a map operation.

        Notes
        -----
        Renames ``old_field`` to ``new_field``.
        """
        data = [{'old_field': 'value'}]
        result = transform(data, {'map': {'old_field': 'new_field'}})
        assert isinstance(result, list)
        assert len(result) == 1
        assert 'new_field' in result[0]

    def test_with_multiple_aggregates(self) -> None:
        """
        Transform with multiple aggregations.

        Notes
        -----
        Produces both sum and count results.
        """
        data = [
            {'value': 1},
            {'value': 2},
            {'value': 3},
        ]
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

    def test_with_multiple_filters_and_select(self) -> None:
        """
        Test transforming using multiple filters and a select sequence.

        Notes
        -----
        Filters twice before selecting fields.
        """
        data = [
            {'name': 'John', 'age': 30, 'city': 'New York'},
            {'name': 'Jane', 'age': 25, 'city': 'Newark'},
            {'name': 'Bob', 'age': 35, 'city': 'Boston'},
        ]
        result = transform(
            data,
            {
                'filter': [
                    {'field': 'age', 'op': 'gte', 'value': 26},
                    {
                        'field': 'city',
                        'op': (
                            lambda value, prefix: str(value).startswith(prefix)
                        ),
                        'value': 'New',
                    },
                ],
                'select': [{'fields': ['name']}],
            },
        )
        assert result == [{'name': 'John'}]

    def test_with_select(self) -> None:
        """
        Test transforming using a select operation.

        Notes
        -----
        Keeps only ``name`` and ``age`` fields.
        """
        data = [{'name': 'John', 'age': 30, 'city': 'NYC'}]
        result = transform(data, {'select': ['name', 'age']})
        assert isinstance(result, list)
        assert len(result) == 1
        assert set(result[0].keys()) == {'name', 'age'}

    def test_with_sort(self) -> None:
        """
        Test transforming using a sort operation.

        Notes
        -----
        Sorts by ``age`` ascending.
        """
        data = [
            {'name': 'John', 'age': 30},
            {'name': 'Jane', 'age': 25},
        ]
        result = transform(data, {'sort': {'field': 'age'}})
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]['age'] == 25

    def test_transform_pipeline(self) -> None:
        """Transform should apply operations in sequence."""

        data = [
            {'name': 'Ada', 'age': 10},
            {'name': 'Bob', 'age': 20},
        ]

        ops: dict[StepType, Any] = {
            'filter': {
                'field': 'age',
                'op': 'gte',
                'value': 15,
            },
            'map': {'name': 'person'},
            'select': ['person', 'age'],
            'sort': {'field': 'age'},
        }

        result = transform(data, ops)

        assert result == [{'person': 'Bob', 'age': 20}]


@pytest.mark.unit
class TestNormalizationHelpers:
    """Unit tests for private normalization helpers."""

    @pytest.mark.parametrize(
        'value,expected',
        [
            (['name', 'age'], True),
            (('city',), True),
            (['name', {'nested': 'no'}], False),
            ('name', False),
        ],
    )
    def test_is_plain_fields_list(self, value: object, expected: bool) -> None:
        """Test only plain sequences of non-mappings return ``True``."""

        assert _is_plain_fields_list(value) is expected

    def test_normalize_operation_keys_accepts_enums(self) -> None:
        """Test :class:`PipelineStep` keys normalizing to lowercase strings."""

        operations = {
            PipelineStep.FILTER: {'field': 'age', 'op': 'gt', 'value': 20},
            'map': {'old': 'new'},
        }

        normalized = _normalize_operation_keys(operations)

        assert set(normalized) == {'filter', 'map'}
        assert normalized['filter']['field'] == 'age'

    def test_normalize_specs_handles_scalar_and_sequence(self) -> None:
        """Test helper coercing scalars to list and keep sequences."""

        single = {'field': 'age'}
        assert _normalize_specs(None) == []
        assert _normalize_specs(single) == [single]
        assert _normalize_specs([single, single]) == [single, single]
