"""Tests for validate module."""

import json
import tempfile
import pytest
from pathlib import Path
from etlplus.validate import validate, validate_field, load_data


def test_validate_field_required():
    """Test field required validation."""
    result = validate_field(None, {"required": True})
    assert not result["valid"]
    assert "required" in result["errors"][0].lower()


def test_validate_field_type_string():
    """Test field type validation for string."""
    result = validate_field("test", {"type": "string"})
    assert result["valid"]
    
    result = validate_field(123, {"type": "string"})
    assert not result["valid"]


def test_validate_field_type_number():
    """Test field type validation for number."""
    result = validate_field(123, {"type": "number"})
    assert result["valid"]
    
    result = validate_field(123.45, {"type": "number"})
    assert result["valid"]
    
    result = validate_field("123", {"type": "number"})
    assert not result["valid"]


def test_validate_field_min_max():
    """Test min/max validation for numbers."""
    result = validate_field(5, {"min": 1, "max": 10})
    assert result["valid"]
    
    result = validate_field(0, {"min": 1})
    assert not result["valid"]
    
    result = validate_field(11, {"max": 10})
    assert not result["valid"]


def test_validate_field_length():
    """Test length validation for strings."""
    result = validate_field("hello", {"minLength": 3, "maxLength": 10})
    assert result["valid"]
    
    result = validate_field("hi", {"minLength": 3})
    assert not result["valid"]
    
    result = validate_field("hello world!", {"maxLength": 10})
    assert not result["valid"]


def test_validate_field_enum():
    """Test enum validation."""
    result = validate_field("red", {"enum": ["red", "green", "blue"]})
    assert result["valid"]
    
    result = validate_field("yellow", {"enum": ["red", "green", "blue"]})
    assert not result["valid"]


def test_validate_dict_data():
    """Test validating dictionary data."""
    data = {"name": "John", "age": 30}
    rules = {
        "name": {"type": "string", "required": True},
        "age": {"type": "number", "min": 0, "max": 150}
    }
    
    result = validate(data, rules)
    assert result["valid"]
    assert result["data"] == data


def test_validate_dict_data_with_errors():
    """Test validating dictionary data with errors."""
    data = {"name": 123, "age": 200}
    rules = {
        "name": {"type": "string", "required": True},
        "age": {"type": "number", "min": 0, "max": 150}
    }
    
    result = validate(data, rules)
    assert not result["valid"]
    assert len(result["errors"]) > 0


def test_validate_list_data():
    """Test validating list data."""
    data = [
        {"name": "John", "age": 30},
        {"name": "Jane", "age": 25}
    ]
    rules = {
        "name": {"type": "string", "required": True},
        "age": {"type": "number", "min": 0}
    }
    
    result = validate(data, rules)
    assert result["valid"]


def test_validate_no_rules():
    """Test validation without rules."""
    data = {"test": "data"}
    result = validate(data)
    assert result["valid"]
    assert result["data"] == data


def test_validate_from_json_string():
    """Test validation from JSON string."""
    json_str = '{"name": "John", "age": 30}'
    result = validate(json_str)
    assert result["valid"]
    assert result["data"]["name"] == "John"


def test_validate_from_file():
    """Test validation from file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        test_data = {"name": "John", "age": 30}
        json.dump(test_data, f)
        temp_path = f.name
    
    try:
        result = validate(temp_path)
        assert result["valid"]
        assert result["data"] == test_data
    finally:
        Path(temp_path).unlink()


def test_load_data_invalid_source():
    """Test loading data from invalid source."""
    with pytest.raises(ValueError, match="Invalid data source"):
        load_data("not a valid json string")
