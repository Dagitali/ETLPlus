"""
:mod:`tests.unit.ops.test_u_ops_validate` module.

Unit tests for :mod:`etlplus.ops.validate`.

Notes
-----
- Exercises type, required, and range checks on fields.
- Uses temporary files to verify load/validate convenience helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from etlplus.ops.validate import FieldRules
from etlplus.ops.validate import load_data
from etlplus.ops.validate import validate
from etlplus.ops.validate import validate_field
from etlplus.types import JSONData

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestLoadData:
    """
    Unit test suite for :func:`etlplus.ops.validate.load_data`.
    """

    def test_invalid_source(self) -> None:
        """Invalid input string should raise ValueError during loading."""
        with pytest.raises(ValueError, match='Invalid data source'):
            load_data('not a valid json string')


@pytest.mark.unit
class TestValidateField:
    """Unit test suite for :func:`etlplus.ops.validate.validate_field`."""

    def test_enum_rule_requires_list(self) -> None:
        """Test non-list enum rules adding an error entry."""

        # Test expects the value for key ``enum`` to not be a list.
        result = validate_field('a', {'enum': 'abc'})  # type: ignore
        assert result['valid'] is False
        assert any('enum' in err for err in result['errors'])

    def test_pattern_rule_with_invalid_regex(self) -> None:
        """Test invalid regex patterns adding an error entry."""

        result = validate_field('abc', {'pattern': '['})
        assert result['valid'] is False
        assert any('pattern' in err for err in result['errors'])

    def test_required_error_message(self) -> None:
        """Validate error message for required field."""
        result = validate_field(None, {'required': True})
        assert 'required' in result['errors'][0].lower()

    @pytest.mark.parametrize(
        'value, rule, expected_valid',
        [
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
    ) -> None:
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


@pytest.mark.unit
class TestValidate:
    """Unit test suite for :func:`etlplus.ops.validate.validate`."""

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
    def test_dict_and_list(
        self,
        data: Any,
        rules: dict[str, Any],
        expected_valid: bool,
    ) -> None:
        """
        Test dict and list data against rules.

        Parameters
        ----------
        data : Any
            Data to validate.
        rules : dict[str, Any]
            Validation rules.
        expected_valid : bool
            Expected validity result.
        """
        result = validate(data, rules)
        assert result['valid'] is expected_valid

    def test_from_file(
        self,
        temp_json_file: Callable[[JSONData], Path],
    ) -> None:
        """
        Test from a JSON file path.

        Parameters
        ----------
        temp_json_file : Callable[[JSONData], Path]
            Fixture to create a temp JSON file in a pytest-managed directory.
        """
        test_data = {'name': 'John', 'age': 30}
        temp_path = temp_json_file(test_data)
        result = validate(temp_path)
        assert result['valid']
        assert result['data'] == test_data

    def test_from_json_string(self) -> None:
        """Test from a JSON string."""
        json_str = '{"name": "John", "age": 30}'
        result = validate(json_str)
        assert result['valid']
        data = result['data']
        if isinstance(data, dict):
            assert data['name'] == 'John'
        elif isinstance(data, list):
            assert any(d.get('name') == 'John' for d in data)

    def test_list_with_non_dict_items(self) -> None:
        """Test lists containing non-dicts recording item-level errors."""

        payload: list[Any] = [{'name': 'Ada'}, 'bad']
        rules: dict[str, FieldRules] = {'name': {'type': 'string'}}
        result = validate(payload, rules)
        assert result['valid'] is False
        assert '[1]' in result['field_errors']

    def test_no_rules(self) -> None:
        """Test without rules returns the data unchanged."""
        data = {'test': 'data'}
        result = validate(data)
        assert result['valid']
        assert result['data'] == data

    def test_validate_handles_load_errors(self) -> None:
        """Test invalid sources reporting errors via the errors collection."""

        rules: dict[str, FieldRules] = {'name': {'required': True}}
        result = validate('not json', rules)
        assert result['valid'] is False
        assert result['data'] is None
        assert any('Failed to load data' in err for err in result['errors'])
