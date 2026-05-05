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
        calls: list[tuple[str, str, str | None, bool]] = []

        def _importer(dependency_name: str) -> object:
            calls.append((dependency_name, 'JSON Schema', None, False))
            return sentinel

        monkeypatch.setattr(ops_imports_mod._DEPENDENCY_IMPORTER, 'cache', {})
        monkeypatch.setattr(
            ops_imports_mod._DEPENDENCY_IMPORTER,
            'importer',
            _importer,
        )

        assert (
            ops_imports_mod.get_dependency(
                'jsonschema',
                format_name='JSON Schema',
            )
            is sentinel
        )
        assert calls == [('jsonschema', 'JSON Schema', None, False)]

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

    def test_validate_schema_infers_frictionless_from_csv_source_path(
        self,
        tmp_path: Path,
    ) -> None:
        """Schema validation should infer Frictionless from a CSV source path."""
        pytest.importorskip('frictionless')

        schema_path = tmp_path / 'schema.json'
        csv_path = tmp_path / 'sample.csv'
        schema_path.write_text(
            '{"fields": [{"name": "name", "type": "string"}]}',
            encoding='utf-8',
        )
        csv_path.write_text('name\nAda\n', encoding='utf-8')

        result = validate_schema(csv_path, schema_path)

        assert result['valid'] is True
        assert result['errors'] == []

    def test_validate_schema_infers_jsonschema_from_source_format(self) -> None:
        """Schema validation should infer JSON Schema from a JSON-like source hint."""
        pytest.importorskip('jsonschema')

        result = validate_schema(
            '{"name": "Ada"}',
            '{"type": "object", "properties": {"name": {"type": "string"}}}',
            source_format='json',
        )

        assert result['valid'] is True
        assert result['errors'] == []

    def test_validate_schema_reports_unsupported_format(self) -> None:
        """Schema validation should reject unsupported schema formats."""
        result = validate_schema('<root />', '<schema />', schema_format='rng')
        assert result['valid'] is False
        assert any('Unsupported schema format' in err for err in result['errors'])

    def test_validate_schema_requires_explicit_format_when_ambiguous(self) -> None:
        """Schema validation should fail clearly when format inference is ambiguous."""
        result = validate_schema(
            '{"name": "Ada"}',
            '{"type": "object"}',
        )
        assert result['valid'] is False
        assert any(
            'Schema format could not be inferred' in err for err in result['errors']
        )

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

    def test_validate_schema_with_jsonschema_reports_missing_source_path(self) -> None:
        """JSON Schema validation should report missing path-like sources clearly."""
        pytest.importorskip('jsonschema')

        result = validate_schema(
            'missing.json',
            '{"type": "object"}',
            schema_format='jsonschema',
        )

        assert result['valid'] is False
        assert any('Source not found: missing.json' in err for err in result['errors'])

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

    def test_frictionless_cleans_up_existing_source_failures(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Existing CSV source failures should not require temporary cleanup."""
        csv_path = tmp_path / 'source.csv'
        csv_path.write_text('name\nAda\n', encoding='utf-8')

        class FakeFrictionless:
            """Tiny Frictionless double that raises while validating a resource."""

            class FrictionlessException(Exception):
                """Fake Frictionless exception."""

            class Schema:
                """Fake schema factory."""

                @staticmethod
                def from_descriptor(_descriptor: object) -> object:
                    return object()

            class Resource:
                """Fake resource facade."""

                def __init__(self, **_kwargs: object) -> None:
                    pass

                def validate(self) -> object:
                    raise OSError('resource failed')

        monkeypatch.setattr(validate_mod, 'get_frictionless', lambda: FakeFrictionless)

        result = validate_mod._validate_frictionless(
            csv_path,
            '{"fields": [{"name": "name", "type": "string"}]}',
            'csv',
        )

        assert result['valid'] is False
        assert result['errors'] == ['CSV schema validation failed: resource failed']

    def test_frictionless_reports_schema_load_and_validation_failures(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """CSV schema validation should normalize schema-load and runtime failures."""
        result = validate_mod._validate_frictionless(
            'name\nAda\n',
            'missing.json',
            'csv',
        )
        assert result['valid'] is False
        assert result['errors'] == ['Schema not found: missing.json']

        class FakeFrictionless:
            """Tiny Frictionless double that raises during descriptor parsing."""

            class FrictionlessException(Exception):
                """Fake Frictionless exception."""

            class Schema:
                """Fake schema factory."""

                @staticmethod
                def from_descriptor(_descriptor: object) -> object:
                    raise FakeFrictionless.FrictionlessException('bad schema')

            class Resource:
                """Unused fake resource."""

        monkeypatch.setattr(validate_mod, 'get_frictionless', lambda: FakeFrictionless)

        failed = validate_mod._validate_frictionless(
            'name\nAda\n',
            '{"fields": [{"name": "name", "type": "string"}]}',
            'csv',
        )

        assert failed['valid'] is False
        assert failed['errors'] == ['CSV schema validation failed: bad schema']

    @pytest.mark.parametrize(
        ('schema', 'expected'),
        [
            pytest.param('customers.table-schema.json', 'frictionless', id='dash'),
            pytest.param(
                'customers.table_schema.json',
                'frictionless',
                id='underscore',
            ),
            pytest.param('customers.tableschema.json', 'frictionless', id='compact'),
            pytest.param('<schema />', None, id='inline'),
        ],
    )
    def test_infer_schema_format_from_path(
        self,
        schema: str,
        expected: str | None,
    ) -> None:
        """Schema format inference should recognize Table Schema file names."""
        assert validate_mod._infer_schema_format_from_path(schema) == expected

    def test_load_jsonschema_document_reports_missing_path_object(
        self,
        tmp_path: Path,
    ) -> None:
        """Path objects should produce clear missing-document errors."""
        missing = tmp_path / 'missing.json'

        with pytest.raises(ValueError, match='Schema not found'):
            validate_mod._load_jsonschema_document(
                missing,
                format_hint='json',
                label='Schema',
            )

    def test_load_jsonschema_document_wraps_file_read_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Existing candidates should wrap file backend missing-file errors."""
        schema_path = tmp_path / 'schema.json'
        schema_path.write_text('{}', encoding='utf-8')

        class MissingFile:
            """File double that raises while reading an existing candidate."""

            def __init__(self, *_args: object, **_kwargs: object) -> None:
                pass

            def read(self) -> object:
                raise FileNotFoundError('gone')

        monkeypatch.setattr(validate_mod, 'File', MissingFile)

        with pytest.raises(ValueError, match='Schema not found'):
            validate_mod._load_jsonschema_document(
                str(schema_path),
                format_hint='json',
                label='Schema',
            )

    def test_jsonschema_formats_array_index_paths(self) -> None:
        """JSON Schema array paths should include bracketed indexes."""
        pytest.importorskip('jsonschema')

        result = validate_schema(
            '[{"name": 1}]',
            (
                '{"type": "array", "items": {"type": "object", '
                '"properties": {"name": {"type": "string"}}}}'
            ),
            schema_format='jsonschema',
        )

        assert result['valid'] is False
        assert '[0].name' in result['field_errors']

    def test_jsonschema_reports_invalid_schema_and_root_errors(self) -> None:
        """JSON Schema validation should handle schema and root-level errors."""
        pytest.importorskip('jsonschema')

        invalid_schema = validate_schema(
            '{}',
            '{"type": 5}',
            schema_format='jsonschema',
        )
        assert invalid_schema['valid'] is False
        assert any('Invalid JSON Schema' in err for err in invalid_schema['errors'])

        root_error = validate_schema(
            '{}',
            '{"type": "array"}',
            schema_format='jsonschema',
        )
        assert root_error['valid'] is False
        assert root_error['field_errors'] == {}
        assert root_error['errors'] == ["{} is not of type 'array'"]

    def test_jsonschema_reports_schema_load_runtime_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """JSON Schema validation should normalize schema loader runtime errors."""
        pytest.importorskip('jsonschema')

        def fail_schema_load(
            _value: object,
            *,
            format_hint: str | None,
            label: str,
        ) -> object:
            assert format_hint is None
            assert label == 'Schema'
            raise RuntimeError('schema loader unavailable')

        monkeypatch.setattr(
            validate_mod,
            '_load_jsonschema_document',
            fail_schema_load,
        )

        result = validate_mod._validate_jsonschema('{}', '{}')

        assert result['valid'] is False
        assert result['errors'] == ['schema loader unavailable']

    def test_parse_structured_text_rejects_unexpected_normalized_format(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Unexpected normalized formats should fail with the generic format error."""
        monkeypatch.setattr(
            validate_mod,
            '_normalize_jsonschema_format_hint',
            lambda _format_hint: validate_mod.FileFormat.CSV,
        )

        with pytest.raises(
            ValueError,
            match='Unsupported JSON Schema source format: csv',
        ):
            validate_mod._parse_structured_text(
                'a,b\n1,2\n',
                format_hint='csv',
                label='Source',
            )

    @pytest.mark.parametrize(
        ('text', 'format_hint', 'label', 'expected_message'),
        [
            pytest.param(
                '{',
                'json',
                'Source',
                'Failed to parse Source as JSON',
                id='json',
            ),
            pytest.param(
                '{',
                None,
                'Source',
                'Failed to parse Source as YAML',
                id='auto-yaml',
            ),
        ],
    )
    def test_parse_structured_text_reports_parse_errors(
        self,
        text: str,
        format_hint: str | None,
        label: str,
        expected_message: str,
    ) -> None:
        """Structured text parsing should surface format-specific failures."""
        with pytest.raises(ValueError, match=expected_message):
            validate_mod._parse_structured_text(
                text,
                format_hint=format_hint,
                label=label,
            )

    def test_parse_structured_text_reraises_yaml_import_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """YAML parser dependency failures should not be wrapped as bad YAML."""
        monkeypatch.setattr(
            validate_mod,
            'get_yaml',
            lambda: (_ for _ in ()).throw(RuntimeError('install yaml')),
        )

        with pytest.raises(RuntimeError, match='install yaml'):
            validate_mod._parse_structured_text(
                'name: Ada',
                format_hint='yaml',
                label='Source',
            )

    def test_resolve_declared_local_path_suppresses_bad_pathlike_values(self) -> None:
        """Path-like values that fail pathlib coercion should degrade to ``None``."""

        class BadPathLike:
            """Path-like test double that looks non-inline but fails coercion."""

            def lstrip(self) -> str:
                return 'bad'

            def __contains__(self, _value: object) -> bool:
                return False

            def __fspath__(self) -> str:
                raise ValueError('bad path')

        assert (
            validate_mod._resolve_declared_local_path(cast(Any, BadPathLike()))
            is None
        )

    def test_resolve_local_path_or_text_preserves_path_after_suppressed_candidate(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Path inputs should remain path inputs even if declaration lookup fails."""
        path = tmp_path / 'missing.xml'
        monkeypatch.setattr(
            validate_mod,
            '_resolve_declared_local_path',
            lambda _value: None,
        )

        assert validate_mod._resolve_local_path_or_text(path) == (path, b'')

    def test_schema_error_path_formatters_cover_nested_and_empty_paths(self) -> None:
        """Schema error paths should format object, index, row, and empty paths."""
        assert validate_mod._format_jsonschema_path(('items', 0, 'name')) == (
            'items[0].name'
        )
        assert validate_mod._format_jsonschema_path(()) is None
        assert (
            validate_mod._format_tabular_error_path(field_name=None, row_number=3)
            == 'row[3]'
        )
        assert (
            validate_mod._format_tabular_error_path(
                field_name=None,
                row_number=None,
            )
            is None
        )

    @pytest.mark.parametrize(
        ('source', 'source_format', 'expected'),
        [
            pytest.param('payload.xml', None, 'xsd', id='xml-suffix'),
            pytest.param('payload.unknown', None, None, id='unsupported-suffix'),
            pytest.param('<root />', None, None, id='inline-xml'),
            pytest.param('payload.csv', 'csv', 'frictionless', id='explicit-csv'),
        ],
    )
    def test_schema_format_resolution_from_source(
        self,
        source: str,
        source_format: str | None,
        expected: str | None,
    ) -> None:
        """Source format inference should drive schema validator selection."""
        if expected is None:
            with pytest.raises(ValueError, match='Schema format could not be inferred'):
                validate_mod._resolve_schema_format(
                    source,
                    'schema',
                    schema_format=None,
                    source_format=source_format,
                )
            return

        assert (
            validate_mod._resolve_schema_format(
                source,
                'schema',
                schema_format=None,
                source_format=source_format,
            )
            == expected
        )

    @pytest.mark.parametrize(
        ('helper_name', 'validator', 'expected_error'),
        [
            pytest.param(
                'get_jsonschema',
                validate_mod._validate_jsonschema,
                'jsonschema missing',
                id='jsonschema',
            ),
            pytest.param(
                'get_frictionless',
                validate_mod._validate_frictionless,
                'frictionless missing',
                id='frictionless',
            ),
            pytest.param(
                'get_lxml_etree',
                validate_mod._validate_xsd,
                'lxml missing',
                id='xsd',
            ),
        ],
    )
    def test_schema_validators_report_missing_dependencies(
        self,
        monkeypatch: pytest.MonkeyPatch,
        helper_name: str,
        validator: Callable[[str, str, str | None], dict[str, object]],
        expected_error: str,
    ) -> None:
        """Schema validators should return structured errors for missing extras."""
        monkeypatch.setattr(
            validate_mod,
            helper_name,
            lambda: (_ for _ in ()).throw(RuntimeError(expected_error)),
        )

        result = validator('{}', '{}', None)

        assert result['valid'] is False
        assert result['errors'] == [expected_error]

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

    def test_frictionless_collects_unscoped_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """CSV schema errors without row or field metadata should stay top-level."""

        class FakeError:
            """Fake Frictionless error without row/field descriptor metadata."""

            def to_descriptor(self) -> dict[str, object]:
                return {'message': 'table failed'}

        class FakeReport:
            """Fake Frictionless report with one unscoped error."""

            tasks = [type('Task', (), {'errors': [FakeError()]})()]

        class FakeFrictionless:
            """Tiny Frictionless double that returns one unscoped error."""

            class FrictionlessException(Exception):
                """Fake Frictionless exception."""

            class Schema:
                """Fake schema factory."""

                @staticmethod
                def from_descriptor(_descriptor: object) -> object:
                    return object()

            class Resource:
                """Fake resource facade."""

                def __init__(self, **_kwargs: object) -> None:
                    pass

                def validate(self) -> FakeReport:
                    return FakeReport()

        monkeypatch.setattr(validate_mod, 'get_frictionless', lambda: FakeFrictionless)

        result = validate_mod._validate_frictionless(
            'name\nAda\n',
            '{"fields": [{"name": "name", "type": "string"}]}',
            'csv',
        )

        assert result['valid'] is False
        assert result['errors'] == ['table failed']
        assert result['field_errors'] == {}

    @pytest.mark.parametrize(
        ('source', 'schema', 'expected'),
        [
            pytest.param('missing.xml', '<xs:schema />', 'XML not found', id='source'),
            pytest.param('<root />', 'missing.xsd', 'XSD not found', id='schema'),
        ],
    )
    def test_xsd_reports_missing_paths(
        self,
        source: str,
        schema: str,
        expected: str,
    ) -> None:
        """XSD validation should distinguish missing XML and XSD paths."""
        pytest.importorskip('lxml.etree')

        result = validate_schema(source, schema, schema_format='xsd')

        assert result['valid'] is False
        assert any(expected in err for err in result['errors'])

    def test_xsd_accepts_inline_xml_and_schema(self) -> None:
        """XSD validation should support inline XML and inline XSD text."""
        pytest.importorskip('lxml.etree')
        schema = (
            '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">'
            '<xs:element name="note" type="xs:string" />'
            '</xs:schema>'
        )

        result = validate_schema('<note>Hello</note>', schema, schema_format='xsd')

        assert result['valid'] is True

    def test_xsd_reports_generic_validation_failure_without_error_log(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """XSD validation should fall back when the schema error log is empty."""

        class FakeEtree:
            """Small lxml.etree double for an empty-error-log validation failure."""

            class XMLSchemaParseError(Exception):
                """Fake schema parse error."""

            class XMLSyntaxError(Exception):
                """Fake XML syntax error."""

            @staticmethod
            def fromstring(_text: bytes) -> object:
                return object()

            class XMLSchema:
                """Fake compiled schema that fails without detailed errors."""

                error_log: list[object] = []

                def __init__(self, _schema_doc: object) -> None:
                    pass

                def validate(self, _xml_doc: object) -> bool:
                    return False

        monkeypatch.setattr(validate_mod, 'get_lxml_etree', lambda: FakeEtree)

        result = validate_mod._validate_xsd('<root />', '<schema />')

        assert result['valid'] is False
        assert result['errors'] == ['XML failed schema validation']

    @pytest.mark.parametrize(
        ('source', 'schema', 'expected'),
        [
            pytest.param('<root />', '<root />', 'Invalid XSD', id='bad-xsd'),
            pytest.param('<bad', '<xs:schema />', 'Invalid XML', id='bad-xml'),
        ],
    )
    def test_xsd_reports_parse_errors(
        self,
        source: str,
        schema: str,
        expected: str,
    ) -> None:
        """XSD validation should classify schema and XML syntax errors."""
        pytest.importorskip('lxml.etree')

        result = validate_schema(source, schema, schema_format='xsd')

        assert result['valid'] is False
        assert any(error.startswith(expected) for error in result['errors'])
