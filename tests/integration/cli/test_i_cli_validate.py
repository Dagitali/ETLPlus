"""
:mod:`tests.integration.cli.test_i_cli_validate` module.

Integration-scope smoke tests for the ``etlplus validate`` CLI command.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING
from typing import Any

import pytest

from etlplus.file import File
from etlplus.file import FileFormat
from tests.integration.conftest import REMOTE_STORAGE_ENV_CASES

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonOutputParser
    from tests.integration.cli.conftest import RealRemoteTargetFactory
    from tests.integration.conftest import RemoteStorageHarness
    from tests.integration.conftest import StdinText

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]

# SECTION: CONSTANTS ======================================================== #


VALID_SCHEMA_RESULT = {
    'valid': True,
    'errors': [],
    'field_errors': {},
    'data': None,
}
FRICTIONLESS_SCHEMA_PERSON = json.dumps(
    {
        'fields': [
            {'name': 'name', 'type': 'string'},
            {'name': 'age', 'type': 'integer'},
        ],
    },
    indent=2,
)
FRICTIONLESS_SCHEMA_CONTACT_CONSTRAINTS = json.dumps(
    {
        'fields': [
            {
                'name': 'email',
                'type': 'string',
                'constraints': {'required': True, 'unique': True},
            },
            {
                'name': 'status',
                'type': 'string',
                'constraints': {
                    'required': True,
                    'enum': ['active', 'inactive'],
                },
            },
        ],
    },
    indent=2,
)
JSON_SCHEMA_PERSON = json.dumps(
    {
        'type': 'object',
        'properties': {'name': {'type': 'string'}},
        'required': ['name'],
    },
    indent=2,
)
JSON_SCHEMA_PERSON_WITH_AGE = json.dumps(
    {
        'type': 'object',
        'properties': {
            'name': {'type': 'string'},
            'age': {'type': 'integer', 'minimum': 0},
        },
        'required': ['name', 'age'],
    },
    indent=2,
)
XML_NOTE_PAYLOAD = '<note><title>Hello</title></note>'
XSD_NOTE_SCHEMA = dedent(
    """\
    <?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="note">
        <xs:complexType>
          <xs:sequence>
            <xs:element name="title" type="xs:string" />
          </xs:sequence>
        </xs:complexType>
      </xs:element>
    </xs:schema>
    """,
)


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class SchemaValidationCase:
    """Input case for successful schema-validation CLI coverage."""

    dependency: str
    schema: str
    source: str
    source_suffix: str
    schema_suffix: str = '.json'
    schema_format: str | None = None
    source_format: str | None = None
    use_stdin: bool = False


# SECTION: TESTS ============================================================ #


class TestCliValidate:
    """Smoke tests for the ``etlplus validate`` CLI command."""

    @pytest.mark.parametrize(
        'case',
        [
            pytest.param(
                SchemaValidationCase(
                    dependency='frictionless',
                    schema=FRICTIONLESS_SCHEMA_PERSON,
                    schema_format='frictionless',
                    source='name,age\nAda,37\n',
                    source_suffix='.csv',
                ),
                id='frictionless-csv-file',
            ),
            pytest.param(
                SchemaValidationCase(
                    dependency='frictionless',
                    schema=FRICTIONLESS_SCHEMA_PERSON,
                    schema_format='frictionless',
                    source='name,age\nAda,37\n',
                    source_format='csv',
                    source_suffix='.csv',
                    use_stdin=True,
                ),
                id='frictionless-csv-stdin',
            ),
            pytest.param(
                SchemaValidationCase(
                    dependency='jsonschema',
                    schema=JSON_SCHEMA_PERSON,
                    schema_format='jsonschema',
                    source='{"name": "Ada"}',
                    source_suffix='.json',
                ),
                id='jsonschema-json-file',
            ),
            pytest.param(
                SchemaValidationCase(
                    dependency='jsonschema',
                    schema=JSON_SCHEMA_PERSON_WITH_AGE,
                    schema_format='jsonschema',
                    source='name: Ada\nage: 37\n',
                    source_format='yaml',
                    source_suffix='.yaml',
                    use_stdin=True,
                ),
                id='jsonschema-yaml-stdin',
            ),
            pytest.param(
                SchemaValidationCase(
                    dependency='lxml.etree',
                    schema=XSD_NOTE_SCHEMA,
                    schema_suffix='.xsd',
                    source=XML_NOTE_PAYLOAD,
                    source_suffix='.xml',
                ),
                id='xsd-xml-file',
            ),
        ],
    )
    def test_schema_validation_succeeds(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        stdin_text: StdinText,
        tmp_path: Path,
        case: SchemaValidationCase,
    ) -> None:
        """Schema mode should emit the standard success payload."""
        pytest.importorskip(case.dependency)
        schema_path = tmp_path / f'schema{case.schema_suffix}'
        schema_path.write_text(case.schema, encoding='utf-8')
        args = ['validate', '--schema', str(schema_path)]
        if case.schema_format is not None:
            args.extend(('--schema-format', case.schema_format))
        if case.source_format is not None:
            args.extend(('--source-format', case.source_format))
        if case.use_stdin:
            stdin_text(case.source)
            args.append('-')
        else:
            source_path = tmp_path / f'sample{case.source_suffix}'
            source_path.write_text(case.source, encoding='utf-8')
            args.append(str(source_path))

        code, out, err = cli_invoke(tuple(args))

        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload == VALID_SCHEMA_RESULT

    def test_frictionless_validation_for_csv_constraint_failures(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        tmp_path: Path,
    ) -> None:
        """Schema mode should surface CSV constraint failures with row paths."""
        pytest.importorskip('frictionless')
        source_path = tmp_path / 'sample.csv'
        schema_path = tmp_path / 'schema.json'
        source_path.write_text(
            'email,status\nada@example.com,active\nada@example.com,\n',
            encoding='utf-8',
        )
        schema_path.write_text(
            FRICTIONLESS_SCHEMA_CONTACT_CONSTRAINTS,
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            (
                'validate',
                '--schema',
                str(schema_path),
                '--schema-format',
                'frictionless',
                str(source_path),
            ),
        )

        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload['valid'] is False
        assert 'row[3].email' in payload['field_errors']
        assert 'row[3].status' in payload['field_errors']
        assert any(
            'unique constraint violation' in message
            for message in payload['field_errors']['row[3].email']
        )
        assert any(
            'constraint "required" is "True"' in message
            for message in payload['field_errors']['row[3].status']
        )

    def test_jsonschema_validation_infers_format_for_json_file(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        tmp_path: Path,
    ) -> None:
        """Schema mode should infer JSON Schema for JSON file validation."""
        pytest.importorskip('jsonschema')
        source_path = tmp_path / 'sample.json'
        schema_path = tmp_path / 'schema.json'
        source_path.write_text('{"name": "Ada"}', encoding='utf-8')
        schema_path.write_text(
            '{"type": "object", "properties": {"name": {"type": "string"}}}',
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            (
                'validate',
                '--schema',
                str(schema_path),
                str(source_path),
            ),
        )

        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload['valid'] is True
        assert payload['errors'] == []

    def test_schema_validation_reports_ambiguous_inference(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        stdin_text: StdinText,
        tmp_path: Path,
    ) -> None:
        """Schema mode should report when schema-format inference is ambiguous."""
        schema_path = tmp_path / 'schema.json'
        schema_path.write_text('{"type": "object"}', encoding='utf-8')
        stdin_text('{"name": "Ada"}')

        code, out, err = cli_invoke(
            (
                'validate',
                '--schema',
                str(schema_path),
                '-',
            ),
        )

        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload['valid'] is False
        assert any(
            'Schema format could not be inferred' in message
            for message in payload['errors']
        )

    def test_schema_option_conflicts_with_rules(
        self,
        cli_invoke: CliInvoke,
        rules_json: str,
    ) -> None:
        """Schema mode should reject simultaneous field-rule validation flags."""
        code, out, err = cli_invoke(
            (
                'validate',
                '--rules',
                rules_json,
                '--schema',
                'sample.xsd',
                'sample.xml',
            ),
        )
        assert code == 2
        assert out.strip() == ''
        assert 'Use either --rules or --schema/--schema-format' in err

    def test_schema_validation_output_file(
        self,
        cli_invoke: CliInvoke,
        tmp_path: Path,
    ) -> None:
        """Schema mode should write the JSON result payload when requested."""
        pytest.importorskip('lxml.etree')
        xml_path = tmp_path / 'sample.xml'
        xsd_path = tmp_path / 'sample.xsd'
        output_path = tmp_path / 'validation.json'
        xml_path.write_text(XML_NOTE_PAYLOAD, encoding='utf-8')
        xsd_path.write_text(XSD_NOTE_SCHEMA, encoding='utf-8')

        code, out, err = cli_invoke(
            (
                'validate',
                '--schema',
                str(xsd_path),
                '--output',
                str(output_path),
                str(xml_path),
            ),
        )

        assert code == 0
        assert err.strip() == ''
        assert out.strip() == f'ValidationDict result saved to {output_path}'
        assert File(output_path, FileFormat.JSON).read() == VALID_SCHEMA_RESULT

    def test_stdin_payload(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        rules_json: str,
        sample_records_json: str,
        stdin_text: StdinText,
    ) -> None:
        """Test validating a STDIN payload with basic rules."""
        stdin_text(sample_records_json)
        code, out, err = cli_invoke(('validate', '--rules', rules_json, '-'))
        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload['valid'] is True

    @pytest.mark.parametrize(
        'env_name',
        REMOTE_STORAGE_ENV_CASES,
    )
    def test_stdin_payload_to_real_remote_output(
        self,
        cli_invoke: CliInvoke,
        rules_json: str,
        sample_records_json: str,
        sample_records: list[dict[str, Any]],
        real_remote_target_factory: RealRemoteTargetFactory,
        stdin_text: StdinText,
        env_name: str,
    ) -> None:
        """Test validating STDIN data into a real cloud-backed target."""
        target = real_remote_target_factory(env_name, suffix='validate-real')
        stdin_text(sample_records_json)

        code, out, err = cli_invoke(
            ('validate', '--rules', rules_json, '--output', target.uri, '-'),
        )

        assert code == 0
        assert err.strip() == ''
        assert out.strip() == f'ValidationDict result saved to {target.uri}'
        assert File(target.uri, FileFormat.JSON).read() == sample_records

    def test_stdin_payload_to_remote_output(
        self,
        cli_invoke: CliInvoke,
        rules_json: str,
        sample_records_json: str,
        sample_records: list[dict[str, object]],
        remote_storage_harness: RemoteStorageHarness,
        stdin_text: StdinText,
    ) -> None:
        """Test validating STDIN data and writing validated output to a remote URI."""
        target_uri = 's3://bucket/validate-output.json'
        stdin_text(sample_records_json)

        code, out, err = cli_invoke(
            (
                'validate',
                '--rules',
                rules_json,
                '--output',
                target_uri,
                '-',
            ),
        )

        assert code == 0
        assert err.strip() == ''
        assert out.strip() == f'ValidationDict result saved to {target_uri}'
        assert remote_storage_harness.read_json(target_uri) == sample_records
