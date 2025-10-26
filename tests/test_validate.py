"""
ETLPlus Validate Tests
======================

Unit tests for the ETLPlus validation utilities.

Notes
-----
Covers field rules, dict/list validation, and loading from strings and
files.
"""
import json
import tempfile
from pathlib import Path

import pytest

from etlplus.validate import load_data
from etlplus.validate import validate
from etlplus.validate import validate_field


def test_validate_field_required():
    """
    Validate the ``required`` rule.

    Notes
    -----
    A ``None`` value with ``required=True`` should yield an error.
    """
    result = validate_field(None, {'required': True})
    assert not result['valid']
    assert 'required' in result['errors'][0].lower()


def test_validate_field_type_string():
    """
    Validate the ``type='string'`` rule.

    Notes
    -----
    Strings pass; numbers fail.
    """
    result = validate_field('test', {'type': 'string'})
    assert result['valid']

    result = validate_field(123, {'type': 'string'})
    assert not result['valid']


def test_validate_field_type_number():
    """
    Validate the ``type='number'`` rule.

    Notes
    -----
    Integers and floats pass; strings fail.
    """
    result = validate_field(123, {'type': 'number'})
    assert result['valid']

    result = validate_field(123.45, {'type': 'number'})
    assert result['valid']

    result = validate_field('123', {'type': 'number'})
    assert not result['valid']


def test_validate_field_min_max():
    """
    Validate ``min`` and ``max`` numeric bounds.

    Notes
    -----
    Values outside the range should fail.
    """
    result = validate_field(5, {'min': 1, 'max': 10})
    assert result['valid']

    result = validate_field(0, {'min': 1})
    assert not result['valid']

    result = validate_field(11, {'max': 10})
    assert not result['valid']


def test_validate_field_length():
    """
    Validate string length using ``minLength`` and ``maxLength``.

    Notes
    -----
    Shorter or longer strings should fail accordingly.
    """
    result = validate_field('hello', {'minLength': 3, 'maxLength': 10})
    assert result['valid']

    result = validate_field('hi', {'minLength': 3})
    assert not result['valid']

    result = validate_field('hello world!', {'maxLength': 10})
    assert not result['valid']


def test_validate_field_enum():
    """
    Validate the ``enum`` rule.

    Notes
    -----
    Only values present in ``enum`` are accepted.
    """
    result = validate_field('red', {'enum': ['red', 'green', 'blue']})
    assert result['valid']

    result = validate_field('yellow', {'enum': ['red', 'green', 'blue']})
    assert not result['valid']


def test_validate_dict_data():
    """
    Validate a mapping against rules.
    """
    data = {'name': 'John', 'age': 30}
    rules = {
        'name': {'type': 'string', 'required': True},
        'age': {'type': 'number', 'min': 0, 'max': 150},
    }

    result = validate(data, rules)
    assert result['valid']
    assert result['data'] == data


def test_validate_dict_data_with_errors():
    """
    Validate a mapping and expect rule violations.
    """
    data = {'name': 123, 'age': 200}
    rules = {
        'name': {'type': 'string', 'required': True},
        'age': {'type': 'number', 'min': 0, 'max': 150},
    }

    result = validate(data, rules)
    assert not result['valid']
    assert len(result['errors']) > 0


def test_validate_list_data():
    """
    Validate a list of mappings against rules.
    """
    data = [
        {'name': 'John', 'age': 30},
        {'name': 'Jane', 'age': 25},
    ]
    rules = {
        'name': {'type': 'string', 'required': True},
        'age': {'type': 'number', 'min': 0},
    }

    result = validate(data, rules)
    assert result['valid']


def test_validate_no_rules():
    """
    Validate without rules returns the data unchanged.
    """
    data = {'test': 'data'}
    result = validate(data)
    assert result['valid']
    assert result['data'] == data


def test_validate_from_json_string():
    """
    Validate from a JSON string.

    Notes
    -----
    The JSON is parsed and considered valid by default.
    """
    json_str = '{"name": "John", "age": 30}'
    result = validate(json_str)
    assert result['valid']
    assert result['data']['name'] == 'John'


def test_validate_from_file():
    """
    Validate from a JSON file path.

    Notes
    -----
    Writes a temporary file and verifies the parsed output.
    """
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.json', delete=False,
    ) as f:
        test_data = {'name': 'John', 'age': 30}
        json.dump(test_data, f)
        temp_path = f.name

    try:
        result = validate(temp_path)
        assert result['valid']
        assert result['data'] == test_data
    finally:
        Path(temp_path).unlink()


def test_load_data_invalid_source():
    """
    Invalid input string raises ``ValueError`` during loading.

    Raises
    ------
    ValueError
        If the input string is not valid JSON.
    """
    with pytest.raises(ValueError, match='Invalid data source'):
        load_data('not a valid json string')
