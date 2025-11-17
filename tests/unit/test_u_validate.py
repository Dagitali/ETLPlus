"""
``tests.unit.test_u_validate`` module.

Unit tests for ``etlplus.validate``.

Notes
-----
- Exercises type, required, and range checks on fields.
- Uses temporary files to verify load/validate convenience helpers.
"""
import json
import tempfile
from pathlib import Path
from typing import Any
from typing import Callable

import pytest

from etlplus.validate import load_data
from etlplus.validate import validate
from etlplus.validate import validate_field


# SECTION: HELPERS ========================================================== #


@pytest.fixture
def temp_json_file() -> Callable[[dict[str, Any]], str]:
    """
    Write a dictionary to a temporary JSON file and return its path.

    Returns
    -------
    Callable[[dict[str, Any]], str]
        Function that writes a dict to a temporary JSON file and returns its
        path.
    """
    def _write(data: dict) -> str:
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False,
        ) as f:
            json.dump(data, f)
            return f.name

    return _write


# SECTION: TESTS =========================================================== #


class TestLoadData:
    """
    Unit test suite for :func:`etlplus.validate.load_data`.
    """

    def test_load_data_invalid_source(self):
        """
        Invalid input string should raise ValueError during loading.
        """
        with pytest.raises(ValueError, match='Invalid data source'):
            load_data('not a valid json string')


class TestValidateField:
    """
    Unit test suite for :func:`etlplus.validate.validate_field`.
    """

    @pytest.mark.parametrize(
        'value, rule, expected_valid', [
            (None, {'required': True}, False),
            ('test', {'type': 'string'}, True),
            (123, {'type': 'string'}, False),
            (123, {'type': 'number'}, True),
            (123.45, {'type': 'number'}, True),
            ('123', {'type': 'number'}, False),
            (5, {'min': 1, 'max': 10}, True),
            (0, {'min': 1}, False),
            (11, {'max': 10}, False),
            ('hello', {'minLength': 3, 'maxLength': 10}, True),
            ('hi', {'minLength': 3}, False),
            ('hello world!', {'maxLength': 10}, False),
            ('red', {'enum': ['red', 'green', 'blue']}, True),
            ('yellow', {'enum': ['red', 'green', 'blue']}, False),
        ],
    )
    def test_validate_field(
        self,
        value: Any,
        rule: dict[str, Any],
        expected_valid: bool,
    ):
        """
        Validate field rules using parameterized cases.

        Parameters
        ----------
        value : Any
            Value to validate.
        rule : dict[str, Any]
            Validation rule.
        expected_valid : bool
            Expected validity result.
        """
        result = validate_field(value, rule)
        assert result['valid'] is expected_valid

    def test_required_error_message(self):
        """
        Validate error message for required field.
        """
        result = validate_field(None, {'required': True})
        assert 'required' in result['errors'][0].lower()


class TestValidate:
    """
    Test suite for validate function.
    """

    @pytest.mark.parametrize(
        'data, rules, expected_valid',
        [
            (
                {
                    'name': 'John',
                    'age': 30,
                },
                {
                    'name': {'type': 'string', 'required': True},
                    'age': {'type': 'number', 'min': 0, 'max': 150},
                },
                True,
            ),
            (
                {
                    'name': 123,
                    'age': 200,
                },
                {
                    'name': {'type': 'string', 'required': True},
                    'age': {'type': 'number', 'min': 0, 'max': 150},
                },
                False,
            ),
            (
                [
                    {
                        'name': 'John',
                        'age': 30,
                    },
                    {
                        'name': 'Jane',
                        'age,': 25,
                    },
                ],
                {
                    'name': {'type': 'string', 'required': True},
                    'age': {'type': 'number', 'min': 0},
                },
                True,
            ),
        ],
    )
    def test_validate_dict_and_list(
        self,
        data,
        rules,
        expected_valid,
    ):
        """
        Validate dict and list data against rules.
        """
        result = validate(data, rules)
        assert result['valid'] is expected_valid

    def test_validate_no_rules(self):
        """
        Validate without rules returns the data unchanged.
        """
        data = {'test': 'data'}
        result = validate(data)
        assert result['valid']
        assert result['data'] == data

    def test_validate_from_file(
        self,
        temp_json_file,
    ):
        """
        Validate from a JSON file path.
        """
        test_data = {'name': 'John', 'age': 30}
        temp_path = temp_json_file(test_data)
        try:
            result = validate(temp_path)
            assert result['valid']
            assert result['data'] == test_data
        finally:
            Path(temp_path).unlink()

    def test_validate_from_json_string(self):
        """
        Validate from a JSON string.
        """
        json_str = '{"name": "John", "age": 30}'
        result = validate(json_str)
        assert result['valid']
        assert result['data']['name'] == 'John'
