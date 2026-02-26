"""
:mod:`tests.unit.ops.test_u_ops_validate` module.

Unit tests for :mod:`etlplus.ops.validate`.

Notes
-----
- Exercises type, required, and range checks on fields.
- Uses temporary files to verify load/validate convenience helpers.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from etlplus.ops.validate import FieldRulesDict
from etlplus.ops.validate import load_data
from etlplus.ops.validate import validate
from etlplus.ops.validate import validate_field
from etlplus.utils.types import JSONData

# SECTION: TESTS ============================================================ #


validate_mod = importlib.import_module('etlplus.ops.validate')


class TestLoadData:
    """
    Unit tests for :func:`etlplus.ops.validate.load_data`.
    """

    def test_invalid_source(self) -> None:
        """Invalid input string should raise ValueError during loading."""
        with pytest.raises(ValueError, match='Invalid data source'):
            load_data('not a valid json string')


class TestValidateField:
    """Unit tests for :func:`etlplus.ops.validate.validate_field`."""

    def test_boolean_type_branch(self) -> None:
        """Test explicit boolean type branch in type matching."""
        assert validate_field(True, {'type': 'boolean'})['valid'] is True
        assert validate_field(1, {'type': 'boolean'})['valid'] is False

    def test_enum_rule_requires_list(self) -> None:
        """Test non-list enum rules adding an error entry."""

        # Test expects the value for key ``enum`` to not be a list.
        result = validate_field('a', {'enum': 'abc'})  # type: ignore
        assert result['valid'] is False
        assert any('enum' in err for err in result['errors'])

    def test_pattern_rule_type_and_mismatch_paths(self) -> None:
        """Test pattern mismatch and non-string pattern validation paths."""
        mismatch = validate_field('abc', {'pattern': '^z'})
        assert mismatch['valid'] is False
        assert any(
            'does not match pattern' in err for err in mismatch['errors']
        )

        matched = validate_field('abc', {'pattern': '^a'})
        assert matched['valid'] is True

        invalid_type = validate_field('abc', {'pattern': 123})
        assert invalid_type['valid'] is False
        assert any('must be a string' in err for err in invalid_type['errors'])

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
            ValidationDict rule.
        expected_valid : bool
            Expected validity result.
        """
        result = validate_field(value, rule)
        assert result['valid'] is expected_valid


class TestValidate:
    """Unit tests for :func:`etlplus.ops.validate.validate`."""

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
            ValidationDict rules.
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
        rules: dict[str, FieldRulesDict] = {'name': {'type': 'string'}}
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

        rules: dict[str, FieldRulesDict] = {'name': {'required': True}}
        result = validate('not json', rules)
        assert result['valid'] is False
        assert result['data'] is None
        assert any('Failed to load data' in err for err in result['errors'])

    def test_validate_handles_non_record_payloads_from_loader(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Unexpected scalar payloads should return invalid=False result."""
        monkeypatch.setattr(validate_mod, 'load_data', lambda _source: 42)
        result = validate('ignored', {'name': {'required': True}})
        assert result['valid'] is True
        assert result['errors'] == []
        assert result['field_errors'] == {}
        assert result['data'] == 42


class TestValidateInternalHelpers:
    """Unit tests for internal validation helper branches."""

    def test_coerce_rule_invalid_value_appends_error(self) -> None:
        """Rule coercion should append errors on bad casts."""
        errors: list[str] = []
        assert (
            validate_mod._coerce_rule(
                {'min': 'x'},
                'min',
                float,
                'numeric',
                errors,
            )
            is None
        )
        assert errors == ["Rule 'min' must be numeric"]

    def test_coerce_rule_none_value_returns_none_without_errors(self) -> None:
        """Rule coercion should ignore explicit None values."""
        errors: list[str] = []
        assert (
            validate_mod._coerce_rule(
                {'min': None},
                'min',
                float,
                'numeric',
                errors,
            )
            is None
        )
        assert errors == []
