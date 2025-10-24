"""Tests for transform module."""

import json
import tempfile
import pytest
from pathlib import Path
from etlplus.transform import (
    transform,
    apply_filter,
    apply_map,
    apply_select,
    apply_sort,
    apply_aggregate,
)


def test_apply_filter_equal():
    """Test filtering with equal operator."""
    data = [
        {"name": "John", "age": 30},
        {"name": "Jane", "age": 25},
        {"name": "Bob", "age": 30}
    ]
    result = apply_filter(data, {"field": "age", "op": "eq", "value": 30})
    assert len(result) == 2
    assert all(item["age"] == 30 for item in result)


def test_apply_filter_greater_than():
    """Test filtering with greater than operator."""
    data = [
        {"name": "John", "age": 30},
        {"name": "Jane", "age": 25},
        {"name": "Bob", "age": 35}
    ]
    result = apply_filter(data, {"field": "age", "op": "gt", "value": 28})
    assert len(result) == 2


def test_apply_filter_in():
    """Test filtering with in operator."""
    data = [
        {"name": "John", "status": "active"},
        {"name": "Jane", "status": "inactive"},
        {"name": "Bob", "status": "active"}
    ]
    result = apply_filter(data, {"field": "status", "op": "in", "value": ["active", "pending"]})
    assert len(result) == 2


def test_apply_map():
    """Test mapping/renaming fields."""
    data = [
        {"old_name": "John", "age": 30},
        {"old_name": "Jane", "age": 25}
    ]
    result = apply_map(data, {"old_name": "new_name"})
    assert all("new_name" in item for item in result)
    assert all("old_name" not in item for item in result)
    assert result[0]["new_name"] == "John"


def test_apply_select():
    """Test selecting specific fields."""
    data = [
        {"name": "John", "age": 30, "city": "NYC"},
        {"name": "Jane", "age": 25, "city": "LA"}
    ]
    result = apply_select(data, ["name", "age"])
    assert all(set(item.keys()) == {"name", "age"} for item in result)


def test_apply_sort():
    """Test sorting data."""
    data = [
        {"name": "John", "age": 30},
        {"name": "Jane", "age": 25},
        {"name": "Bob", "age": 35}
    ]
    result = apply_sort(data, "age")
    assert result[0]["age"] == 25
    assert result[2]["age"] == 35
    
    result = apply_sort(data, "age", reverse=True)
    assert result[0]["age"] == 35
    assert result[2]["age"] == 25


def test_apply_aggregate_sum():
    """Test sum aggregation."""
    data = [
        {"name": "John", "value": 10},
        {"name": "Jane", "value": 20},
        {"name": "Bob", "value": 15}
    ]
    result = apply_aggregate(data, {"field": "value", "func": "sum"})
    assert result["sum_value"] == 45


def test_apply_aggregate_avg():
    """Test average aggregation."""
    data = [
        {"name": "John", "value": 10},
        {"name": "Jane", "value": 20},
        {"name": "Bob", "value": 15}
    ]
    result = apply_aggregate(data, {"field": "value", "func": "avg"})
    assert result["avg_value"] == 15


def test_apply_aggregate_min_max():
    """Test min/max aggregation."""
    data = [
        {"name": "John", "value": 10},
        {"name": "Jane", "value": 20},
        {"name": "Bob", "value": 15}
    ]
    result = apply_aggregate(data, {"field": "value", "func": "min"})
    assert result["min_value"] == 10
    
    result = apply_aggregate(data, {"field": "value", "func": "max"})
    assert result["max_value"] == 20


def test_apply_aggregate_count():
    """Test count aggregation."""
    data = [
        {"name": "John", "value": 10},
        {"name": "Jane", "value": 20},
        {"name": "Bob", "value": 15}
    ]
    result = apply_aggregate(data, {"field": "value", "func": "count"})
    assert result["count_value"] == 3


def test_transform_with_filter():
    """Test transform with filter operation."""
    data = [
        {"name": "John", "age": 30},
        {"name": "Jane", "age": 25},
    ]
    result = transform(data, {"filter": {"field": "age", "op": "gt", "value": 26}})
    assert len(result) == 1
    assert result[0]["name"] == "John"


def test_transform_with_map():
    """Test transform with map operation."""
    data = [{"old_field": "value"}]
    result = transform(data, {"map": {"old_field": "new_field"}})
    assert "new_field" in result[0]


def test_transform_with_select():
    """Test transform with select operation."""
    data = [{"name": "John", "age": 30, "city": "NYC"}]
    result = transform(data, {"select": ["name", "age"]})
    assert set(result[0].keys()) == {"name", "age"}


def test_transform_with_sort():
    """Test transform with sort operation."""
    data = [
        {"name": "John", "age": 30},
        {"name": "Jane", "age": 25},
    ]
    result = transform(data, {"sort": {"field": "age"}})
    assert result[0]["age"] == 25


def test_transform_with_aggregate():
    """Test transform with aggregate operation."""
    data = [
        {"name": "John", "value": 10},
        {"name": "Jane", "value": 20},
    ]
    result = transform(data, {"aggregate": {"field": "value", "func": "sum"}})
    assert result["sum_value"] == 30


def test_transform_from_json_string():
    """Test transform from JSON string."""
    json_str = '[{"name": "John", "age": 30}]'
    result = transform(json_str, {"select": ["name"]})
    assert len(result) == 1
    assert "age" not in result[0]


def test_transform_from_file():
    """Test transform from file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        test_data = [{"name": "John", "age": 30}]
        json.dump(test_data, f)
        temp_path = f.name
    
    try:
        result = transform(temp_path, {"select": ["name"]})
        assert len(result) == 1
        assert "age" not in result[0]
    finally:
        Path(temp_path).unlink()


def test_transform_no_operations():
    """Test transform without operations."""
    data = [{"name": "John"}]
    result = transform(data)
    assert result == data
