"""
``tests.unit.test_u_transform`` module.

Unit tests for ``etlplus.transform``.

Notes
-----
- Uses small in-memory datasets to validate each operation.
- Ensures stable behavior for edge cases (empty inputs, missing fields).
"""
import json
import tempfile
from pathlib import Path

from etlplus.transform import apply_aggregate
from etlplus.transform import apply_filter
from etlplus.transform import apply_map
from etlplus.transform import apply_select
from etlplus.transform import apply_sort
from etlplus.transform import transform


# SECTION: TESTS =========================================================== #


def test_apply_filter_equal():
    """
    Filter with the ``eq`` operator.

    Notes
    -----
    Ensures two records with ``age == 30`` are returned.
    """
    data = [
        {'name': 'John', 'age': 30},
        {'name': 'Jane', 'age': 25},
        {'name': 'Bob', 'age': 30},
    ]
    result = apply_filter(data, {'field': 'age', 'op': 'eq', 'value': 30})
    assert len(result) == 2
    assert all(item['age'] == 30 for item in result)


def test_apply_filter_greater_than():
    """
    Filter with the ``gt`` operator.

    Notes
    -----
    Ensures values greater than 28 are kept.
    """
    data = [
        {'name': 'John', 'age': 30},
        {'name': 'Jane', 'age': 25},
        {'name': 'Bob', 'age': 35},
    ]
    result = apply_filter(data, {'field': 'age', 'op': 'gt', 'value': 28})
    assert len(result) == 2


def test_apply_filter_in():
    """
    Filter with the ``in`` operator.

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


def test_apply_filter_callable_operator():
    """
    Filter with a custom callable operator.

    Notes
    -----
    Keeps records whose ``name`` contains the letter ``'a'``.
    """

    data = [
        {'name': 'John'},
        {'name': 'Jane'},
        {'name': 'Bob'},
    ]
    result = apply_filter(
        data,
        {
            'field': 'name',
            'op': lambda value, needle: needle in value.lower(),
            'value': 'a',
        },
    )
    assert [item['name'] for item in result] == ['Jane']


def test_apply_map():
    """
    Map/rename fields in each record.

    Notes
    -----
    Renames ``old_name`` to ``new_name`` and preserves other fields.
    """
    data = [
        {'old_name': 'John', 'age': 30},
        {'old_name': 'Jane', 'age': 25},
    ]
    result = apply_map(data, {'old_name': 'new_name'})
    assert all('new_name' in item for item in result)
    assert all('old_name' not in item for item in result)
    assert result[0]['new_name'] == 'John'


def test_apply_select():
    """
    Select a subset of fields from each record.

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


def test_apply_sort():
    """
    Sort records by a field.

    Notes
    -----
    Checks ascending and descending sort by ``age``.
    """
    data = [
        {'name': 'John', 'age': 30},
        {'name': 'Jane', 'age': 25},
        {'name': 'Bob', 'age': 35},
    ]
    result = apply_sort(data, 'age')
    assert result[0]['age'] == 25
    assert result[2]['age'] == 35

    result = apply_sort(data, 'age', reverse=True)
    assert result[0]['age'] == 35
    assert result[2]['age'] == 25


def test_apply_aggregate_sum():
    """
    Aggregate with ``sum``.

    Notes
    -----
    Sums the ``value`` field.
    """
    data = [
        {'name': 'John', 'value': 10},
        {'name': 'Jane', 'value': 20},
        {'name': 'Bob', 'value': 15},
    ]
    result = apply_aggregate(data, {'field': 'value', 'func': 'sum'})
    assert result['sum_value'] == 45


def test_apply_aggregate_avg():
    """
    Aggregate with ``avg``.

    Notes
    -----
    Averages the ``value`` field.
    """
    data = [
        {'name': 'John', 'value': 10},
        {'name': 'Jane', 'value': 20},
        {'name': 'Bob', 'value': 15},
    ]
    result = apply_aggregate(data, {'field': 'value', 'func': 'avg'})
    assert result['avg_value'] == 15


def test_apply_aggregate_min_max():
    """
    Aggregate with ``min`` and ``max``.

    Notes
    -----
    Computes the minimum and maximum over ``value``.
    """
    data = [
        {'name': 'John', 'value': 10},
        {'name': 'Jane', 'value': 20},
        {'name': 'Bob', 'value': 15},
    ]
    result = apply_aggregate(data, {'field': 'value', 'func': 'min'})
    assert result['min_value'] == 10

    result = apply_aggregate(data, {'field': 'value', 'func': 'max'})
    assert result['max_value'] == 20


def test_apply_aggregate_count():
    """
    Aggregate with ``count``.

    Notes
    -----
    Counts the number of records with the field present.
    """
    data = [
        {'name': 'John', 'value': 10},
        {'name': 'Jane', 'value': 20},
        {'name': 'Bob', 'value': 15},
    ]
    result = apply_aggregate(data, {'field': 'value', 'func': 'count'})
    assert result['count_value'] == 3


def test_apply_aggregate_callable_with_alias():
    """
    Aggregate with a callable and custom alias.

    Notes
    -----
    Computes the sum plus count and stores it under ``'score'``.
    """

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


def test_transform_with_filter():
    """
    Transform using a filter operation.

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
    assert len(result) == 1
    assert result[0]['name'] == 'John'


def test_transform_with_multiple_filters_and_select():
    """
    Transform using multiple filters and a select sequence.

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
                    'op': lambda value, prefix: str(value).startswith(prefix),
                    'value': 'New',
                },
            ],
            'select': [{'fields': ['name']}],
        },
    )
    assert result == [{'name': 'John'}]


def test_transform_with_map():
    """
    Transform using a map operation.

    Notes
    -----
    Renames ``old_field`` to ``new_field``.
    """
    data = [{'old_field': 'value'}]
    result = transform(data, {'map': {'old_field': 'new_field'}})
    assert 'new_field' in result[0]


def test_transform_with_select():
    """
    Transform using a select operation.

    Notes
    -----
    Keeps only ``name`` and ``age`` fields.
    """
    data = [{'name': 'John', 'age': 30, 'city': 'NYC'}]
    result = transform(data, {'select': ['name', 'age']})
    assert set(result[0].keys()) == {'name', 'age'}


def test_transform_with_sort():
    """
    Transform using a sort operation.

    Notes
    -----
    Sorts by ``age`` ascending.
    """
    data = [
        {'name': 'John', 'age': 30},
        {'name': 'Jane', 'age': 25},
    ]
    result = transform(data, {'sort': {'field': 'age'}})
    assert result[0]['age'] == 25


def test_transform_with_aggregate():
    """
    Transform using an aggregate operation.

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
    assert result['sum_value'] == 30


def test_transform_with_multiple_aggregates():
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


def test_transform_from_json_string():
    """
    Transform from a JSON string.

    Notes
    -----
    Selects only ``name`` from the provided JSON array string.
    """
    json_str = '[{"name": "John", "age": 30}]'
    result = transform(json_str, {'select': ['name']})
    assert len(result) == 1
    assert 'age' not in result[0]


def test_transform_from_file():
    """
    Transform from a JSON file.

    Notes
    -----
    Writes a temporary JSON file and selects only ``name``.
    """
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.json', delete=False,
    ) as f:
        test_data = [{'name': 'John', 'age': 30}]
        json.dump(test_data, f)
        temp_path = f.name

    try:
        result = transform(temp_path, {'select': ['name']})
        assert len(result) == 1
        assert 'age' not in result[0]
    finally:
        Path(temp_path).unlink()


def test_transform_no_operations():
    """
    Transform without operations returns input unchanged.
    """
    data = [{'name': 'John'}]
    result = transform(data)
    assert result == data
