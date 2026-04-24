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
from typing import cast

import pytest

from etlplus.ops.validate import FieldRulesDict
from etlplus.ops.validate import validate
from etlplus.ops.validate import validate_field
from etlplus.ops.validate import validate_schema
from etlplus.utils._types import JSONData

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


validate_mod = importlib.import_module('etlplus.ops.validate')
ops_imports_mod = importlib.import_module('etlplus.ops._imports')


# SECTION: TESTS ============================================================ #


class TestValidate:
    """Unit tests for :func:`validate`."""

    @pytest.mark.parametrize(
        ('data', 'rules', 'expected_valid'),
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
        Test :func:`validate` using a JSON file path.

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
        """Test :func:`validate` using a JSON string."""
        json_str = '{"name": "John", "age": 30}'
        result = validate(json_str)
        assert result['valid']
        data = result['data']
        if isinstance(data, dict):
            assert data['name'] == 'John'
        elif isinstance(data, list):
            assert any(d.get('name') == 'John' for d in data)

    def test_list_with_non_dict_items(self) -> None:
        """Test :func:`validate` with lists containing non-dict items."""

        payload: list[Any] = [{'name': 'Ada'}, 'bad']
        rules: dict[str, FieldRulesDict] = {'name': {'type': 'string'}}
        result = validate(payload, rules)
        assert result['valid'] is False
        assert '[1]' in result['field_errors']

    def test_no_rules(self) -> None:
        """Test that without rules returns the data unchanged."""
        data = {'test': 'data'}
        result = validate(data)
        assert result['valid']
        assert result['data'] == data

    def test_ops_get_dependency_delegates_to_shared_importer(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ops dependency imports should delegate to the shared import helper."""
        sentinel = object()
        calls: list[tuple[str, str, object, object, object, object]] = []

        def _message_builder(
            module_name: str,
            format_name: str,
            pip_name: str | None = None,
            *,
            required: bool = False,
        ) -> str:
            assert module_name == 'jsonschema'
            assert format_name == 'JSON Schema'
            assert pip_name is None
            assert required is False
            return 'jsonschema is required'

        def _import_package(
            dependency_name: str,
            *,
            error_message: str,
            cache: object,
            importer: object,
            error_type: object,
            import_exceptions: object,
        ) -> object:
            calls.append(
                (
                    dependency_name,
                    error_message,
                    cache,
                    importer,
                    error_type,
                    import_exceptions,
                ),
            )
            return sentinel

        monkeypatch.setattr(
            ops_imports_mod,
            'build_dependency_error_message',
            _message_builder,
        )
        monkeypatch.setattr(ops_imports_mod, 'import_package', _import_package)

        assert (
            ops_imports_mod.get_dependency(
                'jsonschema',
                format_name='JSON Schema',
            )
            is sentinel
        )
        assert calls == [
            (
                'jsonschema',
                'jsonschema is required',
                ops_imports_mod._MODULE_CACHE,
                ops_imports_mod.import_module,
                RuntimeError,
                Exception,
            ),
        ]

    @pytest.mark.parametrize(
        ('helper_name', 'module_name', 'format_name', 'pip_name'),
        [
            (
                'get_frictionless',
                'frictionless',
                'CSV schema',
                None,
            ),
            (
                'get_jsonschema',
                'jsonschema',
                'JSON Schema',
                None,
            ),
            (
                'get_lxml_etree',
                'lxml.etree',
                'XML schema',
                'lxml',
            ),
            (
                'get_yaml',
                'yaml',
                'YAML schema',
                'PyYAML',
            ),
        ],
        ids=['frictionless', 'jsonschema', 'lxml', 'yaml'],
    )
    def test_schema_import_helpers_delegate_to_get_dependency(
        self,
        monkeypatch: pytest.MonkeyPatch,
        helper_name: str,
        module_name: str,
        format_name: str,
        pip_name: str | None,
    ) -> None:
        """Schema-specific ops helpers should delegate to ``get_dependency``."""
        sentinel = object()
        calls: list[tuple[str, str, str | None, bool]] = []

        def _get_dependency(
            dependency_name: str,
            *,
            format_name: str,
            pip_name: str | None = None,
            required: bool = False,
        ) -> object:
            calls.append((dependency_name, format_name, pip_name, required))
            return sentinel

        monkeypatch.setattr(ops_imports_mod, 'get_dependency', _get_dependency)

        assert getattr(ops_imports_mod, helper_name)() is sentinel
        assert calls == [(module_name, format_name, pip_name, True)]

    def test_validate_handles_load_errors(self) -> None:
        """
        Test that invalid sources report errors via the errors collection.
        """
        rules: dict[str, FieldRulesDict] = {'name': {'required': True}}
        result = validate('not json', rules)
        assert result['valid'] is False
        assert result['data'] is None
        assert any('Failed to load data' in err for err in result['errors'])

    def test_validate_schema_reports_unsupported_format(self) -> None:
        """Schema validation should reject unsupported schema formats."""
        result = validate_schema('<root />', '<schema />', schema_format='rng')
        assert result['valid'] is False
        assert any('Unsupported schema format' in err for err in result['errors'])

    def test_validate_schema_reports_validation_errors(
        self,
        tmp_path: Path,
    ) -> None:
        """Schema validation should report XSD validation failures."""
        pytest.importorskip('lxml.etree')
        xml_path = tmp_path / 'sample.xml'
        xsd_path = tmp_path / 'sample.xsd'
        xml_path.write_text(
            '<note><body>Hello</body></note>',
            encoding='utf-8',
        )
        xsd_path.write_text(
            '\n'.join(
                [
                    '<?xml version="1.0" encoding="UTF-8"?>',
                    '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">',
                    '  <xs:element name="note">',
                    '    <xs:complexType>',
                    '      <xs:sequence>',
                    '        <xs:element name="title" type="xs:string" />',
                    '      </xs:sequence>',
                    '    </xs:complexType>',
                    '  </xs:element>',
                    '</xs:schema>',
                ],
            ),
            encoding='utf-8',
        )

        result = validate_schema(xml_path, xsd_path)
        assert result['valid'] is False
        assert any(error.startswith('Line ') for error in result['errors'])

    def test_validate_schema_with_frictionless_collects_constraint_errors(
        self,
    ) -> None:
        """Schema validation should retain row paths for CSV constraints."""
        pytest.importorskip('frictionless')

        schema = '\n'.join(
            [
                '{',
                '  "fields": [',
                (
                    '    {"name": "email", "type": "string", "constraints": '
                    '{"required": true, "unique": true}},'
                ),
                (
                    '    {"name": "status", "type": "string", "constraints": '
                    '{"required": true, "enum": ["active", "inactive"]}}'
                ),
                '  ]',
                '}',
            ],
        )
        payload = 'email,status\nada@example.com,active\nada@example.com,\n'

        result = validate_schema(
            payload,
            schema,
            schema_format='frictionless',
            source_format='csv',
        )

        assert result['valid'] is False
        assert 'row[3].email' in result['field_errors']
        assert 'row[3].status' in result['field_errors']
        assert any(
            'unique constraint violation' in message
            for message in result['field_errors']['row[3].email']
        )
        assert any(
            'constraint "required" is "True"' in message
            for message in result['field_errors']['row[3].status']
        )

    def test_validate_schema_with_frictionless_csv_text(self) -> None:
        """Schema validation should accept valid CSV text against Table Schema."""
        pytest.importorskip('frictionless')

        schema = '\n'.join(
            [
                '{',
                '  "fields": [',
                '    {"name": "name", "type": "string"},',
                '    {"name": "age", "type": "integer"}',
                '  ]',
                '}',
            ],
        )
        payload = 'name,age\nAda,37\n'

        result = validate_schema(
            payload,
            schema,
            schema_format='frictionless',
            source_format='csv',
        )

        assert result['valid'] is True
        assert result['errors'] == []
        assert result['field_errors'] == {}
        assert result['data'] is None

    def test_validate_schema_with_frictionless_collects_field_errors(
        self,
        tmp_path: Path,
    ) -> None:
        """CSV schema validation should retain row and field error paths."""
        pytest.importorskip('frictionless')

        schema_path = tmp_path / 'schema.json'
        csv_path = tmp_path / 'sample.csv'
        schema_path.write_text(
            '\n'.join(
                [
                    '{',
                    '  "fields": [',
                    '    {"name": "name", "type": "string"},',
                    '    {"name": "age", "type": "integer"}',
                    '  ]',
                    '}',
                ],
            ),
            encoding='utf-8',
        )
        csv_path.write_text(
            'name,age\nAda,37\nBob,not-a-number\n',
            encoding='utf-8',
        )

        result = validate_schema(
            csv_path,
            schema_path,
            schema_format='frictionless',
        )

        assert result['valid'] is False
        assert 'row[3].age' in result['field_errors']
        assert any(error.startswith('row[3].age: ') for error in result['errors'])

    def test_validate_schema_with_frictionless_collects_header_errors(
        self,
    ) -> None:
        """Schema validation should retain header-level field paths."""
        pytest.importorskip('frictionless')

        schema = '\n'.join(
            [
                '{',
                '  "fields": [',
                '    {"name": "name", "type": "string"},',
                '    {"name": "age", "type": "integer"}',
                '  ]',
                '}',
            ],
        )
        payload = 'name\nAda\n'

        result = validate_schema(
            payload,
            schema,
            schema_format='frictionless',
            source_format='csv',
        )

        assert result['valid'] is False
        assert 'age' in result['field_errors']
        assert 'row[2].age' in result['field_errors']
        assert any(error.startswith('age: ') for error in result['errors'])
        assert any(error.startswith('row[2].age: ') for error in result['errors'])

    def test_validate_schema_with_frictionless_rejects_bad_source_format(
        self,
    ) -> None:
        """CSV schema validation should reject unsupported payload formats."""
        pytest.importorskip('frictionless')

        schema = '{"fields": [{"name": "name", "type": "string"}]}'
        result = validate_schema(
            'name\nAda\n',
            schema,
            schema_format='frictionless',
            source_format='yaml',
        )

        assert result['valid'] is False
        assert any(
            'Unsupported CSV schema source format: yaml' in error
            for error in result['errors']
        )

    def test_validate_schema_with_jsonschema_json(self) -> None:
        """Schema validation should accept valid JSON against JSON Schema."""
        pytest.importorskip('jsonschema')

        schema = '\n'.join(
            [
                '{',
                '  "type": "object",',
                '  "properties": {"name": {"type": "string"}},',
                '  "required": ["name"]',
                '}',
            ],
        )
        payload = '{"name": "Ada"}'

        result = validate_schema(
            payload,
            schema,
            schema_format='jsonschema',
        )

        assert result['valid'] is True
        assert result['errors'] == []
        assert result['field_errors'] == {}
        assert result['data'] is None

    def test_validate_schema_with_jsonschema_yaml(self) -> None:
        """Schema validation should accept valid YAML against JSON Schema."""
        pytest.importorskip('jsonschema')

        schema = '\n'.join(
            [
                '{',
                '  "type": "object",',
                '  "properties": {',
                '    "name": {"type": "string"},',
                '    "age": {"type": "integer", "minimum": 0}',
                '  },',
                '  "required": ["name", "age"]',
                '}',
            ],
        )
        payload = 'name: Ada\nage: 37\n'

        result = validate_schema(
            payload,
            schema,
            schema_format='jsonschema',
            source_format='yaml',
        )

        assert result['valid'] is True
        assert result['errors'] == []
        assert result['field_errors'] == {}
        assert result['data'] is None

    def test_validate_schema_with_jsonschema_collects_field_errors(self) -> None:
        """Schema validation should retain field paths for JSON Schema errors."""
        pytest.importorskip('jsonschema')

        schema = '\n'.join(
            [
                '{',
                '  "type": "object",',
                '  "properties": {"name": {"type": "string"}},',
                '  "required": ["name"]',
                '}',
            ],
        )
        payload = '{"name": 42}'

        result = validate_schema(
            payload,
            schema,
            schema_format='jsonschema',
        )

        assert result['valid'] is False
        assert result['field_errors'] == {'name': ["42 is not of type 'string'"]}
        assert any(error.startswith('name: ') for error in result['errors'])

    def test_validate_schema_with_jsonschema_rejects_bad_source_format(self) -> None:
        """Schema validation should reject unsupported JSON Schema payload formats."""
        pytest.importorskip('jsonschema')

        schema = '{"type": "object"}'
        result = validate_schema(
            '{"name": "Ada"}',
            schema,
            schema_format='jsonschema',
            source_format='csv',
        )

        assert result['valid'] is False
        assert any(
            'Unsupported JSON Schema source format: csv' in error
            for error in result['errors']
        )

    def test_validate_schema_with_xsd(
        self,
        tmp_path: Path,
    ) -> None:
        """Schema validation should accept matching XML/XSD pairs."""
        pytest.importorskip('lxml.etree')
        xml_path = tmp_path / 'sample.xml'
        xsd_path = tmp_path / 'sample.xsd'
        xml_path.write_text(
            '<note><title>Hello</title></note>',
            encoding='utf-8',
        )
        xsd_path.write_text(
            '\n'.join(
                [
                    '<?xml version="1.0" encoding="UTF-8"?>',
                    '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">',
                    '  <xs:element name="note">',
                    '    <xs:complexType>',
                    '      <xs:sequence>',
                    '        <xs:element name="title" type="xs:string" />',
                    '      </xs:sequence>',
                    '    </xs:complexType>',
                    '  </xs:element>',
                    '</xs:schema>',
                ],
            ),
            encoding='utf-8',
        )

        result = validate_schema(xml_path, xsd_path)
        assert result['valid'] is True
        assert result['errors'] == []
        assert result['field_errors'] == {}
        assert result['data'] is None


class TestValidateField:
    """Unit tests for :func:`validate_field`."""

    def test_boolean_type_branch(self) -> None:
        """Test that explicit boolean type branch in type matches."""
        assert validate_field(True, {'type': 'boolean'})['valid'] is True
        assert validate_field(1, {'type': 'boolean'})['valid'] is False

    def test_enum_rule_requires_list(self) -> None:
        """Test that non-list enum rules add an error entry."""

        # Test expects the value for key ``enum`` to not be a list.
        result = validate_field('a', {'enum': 'abc'})  # type: ignore
        assert result['valid'] is False
        assert any('enum' in err for err in result['errors'])

    def test_integer_type_branch(self) -> None:
        """Test that integer type excludes booleans."""
        assert validate_field(7, {'type': 'integer'})['valid'] is True
        assert validate_field(True, {'type': 'integer'})['valid'] is False

    def test_pattern_rule_type_and_mismatch_paths(self) -> None:
        """Test pattern mismatch and non-string pattern validation paths."""
        mismatch = validate_field('abc', {'pattern': '^z'})
        assert mismatch['valid'] is False
        assert any('does not match pattern' in err for err in mismatch['errors'])

        matched = validate_field('abc', {'pattern': '^a'})
        assert matched['valid'] is True

        invalid_type = validate_field(
            'abc',
            cast(dict[str, Any], {'pattern': 123}),
        )
        assert invalid_type['valid'] is False
        assert any('must be a string' in err for err in invalid_type['errors'])

    def test_pattern_rule_with_invalid_regex(self) -> None:
        """Test that invalid regex patterns add an error entry."""

        result = validate_field('abc', {'pattern': '['})
        assert result['valid'] is False
        assert any('pattern' in err for err in result['errors'])

    def test_required_error_message(self) -> None:
        """Test error message for required field."""
        result = validate_field(None, {'required': True})
        assert 'required' in result['errors'][0].lower()

    @pytest.mark.parametrize(
        ('value', 'rule', 'expected_valid'),
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
        """Test field rules using parameterized cases."""
        result = validate_field(value, rule)
        assert result['valid'] is expected_valid


class TestValidateInternalHelpers:
    """Unit tests for internal validation helper branches."""

    def test_coerce_rule_invalid_value_appends_error(self) -> None:
        """Test that rule coercion appends errors on bad casts."""
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
        """Test that rule coercion ignores explicit None values."""
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
        assert not errors

    @pytest.mark.parametrize(
        ('value', 'expected_type', 'expected'),
        [
            ('name', 'string', True),
            (7, 'number', True),
            (7, 'integer', True),
            (True, 'integer', False),
            (True, 'boolean', True),
            ([{'id': 1}], 'array', True),
            ({'id': 1}, 'object', True),
            ('name', 'unknown', False),
        ],
    )
    def test_type_matches_matrix(
        self,
        value: Any,
        expected_type: str,
        expected: bool,
    ) -> None:
        """Type matching should respect the repo's JSON-like type rules."""
        assert validate_mod._type_matches(value, expected_type) is expected
