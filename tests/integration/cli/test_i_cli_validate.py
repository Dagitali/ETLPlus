"""
:mod:`tests.integration.cli.test_i_cli_validate` module.

Integration-scope smoke tests for the ``etlplus validate`` CLI command.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

import pytest

from etlplus.file import File
from etlplus.file import FileFormat

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonOutputParser
    from tests.integration.cli.conftest import RealRemoteTargetFactory
    from tests.integration.cli.conftest import RemoteStorageHarness

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]

# SECTION: TESTS ============================================================ #


class TestCliValidate:
    """Smoke tests for the ``etlplus validate`` CLI command."""

    def test_jsonschema_validation_for_json_file(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        tmp_path: Path,
    ) -> None:
        """Schema mode should validate JSON files with JSON Schema."""
        pytest.importorskip('jsonschema')
        source_path = tmp_path / 'sample.json'
        schema_path = tmp_path / 'schema.json'
        source_path.write_text('{"name": "Ada"}', encoding='utf-8')
        schema_path.write_text(
            '\n'.join(
                [
                    '{',
                    '  "type": "object",',
                    '  "properties": {"name": {"type": "string"}},',
                    '  "required": ["name"]',
                    '}',
                ],
            ),
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            (
                'validate',
                '--schema',
                str(schema_path),
                '--schema-format',
                'jsonschema',
                str(source_path),
            ),
        )

        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload['valid'] is True
        assert payload['errors'] == []
        assert payload['field_errors'] == {}
        assert payload['data'] is None

    def test_jsonschema_validation_for_yaml_stdin(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Schema mode should honor the source format hint for YAML STDIN."""
        pytest.importorskip('jsonschema')
        schema_path = tmp_path / 'schema.json'
        schema_path.write_text(
            '\n'.join(
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
            ),
            encoding='utf-8',
        )
        monkeypatch.setattr(sys, 'stdin', io.StringIO('name: Ada\nage: 37\n'))

        code, out, err = cli_invoke(
            (
                'validate',
                '--schema',
                str(schema_path),
                '--schema-format',
                'jsonschema',
                '--source-format',
                'yaml',
                '-',
            ),
        )

        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload['valid'] is True
        assert payload['errors'] == []
        assert payload['field_errors'] == {}
        assert payload['data'] is None

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

    def test_schema_validation(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        tmp_path: Path,
    ) -> None:
        """Schema mode should emit the structured validation result payload."""
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

        code, out, err = cli_invoke(
            ('validate', '--schema', str(xsd_path), str(xml_path)),
        )

        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload['valid'] is True
        assert payload['errors'] == []
        assert payload['field_errors'] == {}
        assert payload['data'] is None

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
        assert File(output_path, FileFormat.JSON).read() == {
            'valid': True,
            'errors': [],
            'field_errors': {},
            'data': None,
        }

    def test_stdin_payload(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        rules_json: str,
        sample_records_json: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test validating a STDIN payload with basic rules."""
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))
        code, out, err = cli_invoke(('validate', '--rules', rules_json, '-'))
        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload['valid'] is True

    @pytest.mark.parametrize(
        ('env_name', 'backend_label'),
        [
            ('ETLPLUS_TEST_S3_URI', 's3'),
            ('ETLPLUS_TEST_AZURE_BLOB_URI', 'azure-blob'),
        ],
        ids=['s3', 'azure-blob'],
    )
    def test_stdin_payload_to_real_remote_output(
        self,
        cli_invoke: CliInvoke,
        rules_json: str,
        sample_records_json: str,
        sample_records: list[dict[str, Any]],
        real_remote_target_factory: RealRemoteTargetFactory,
        monkeypatch: pytest.MonkeyPatch,
        env_name: str,
        backend_label: str,
    ) -> None:
        """Test validating STDIN data into a real cloud-backed target."""
        del backend_label
        target = real_remote_target_factory(env_name, suffix='validate-real')
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))

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
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test validating STDIN data and writing validated output to a remote URI."""
        target_uri = 's3://bucket/validate-output.json'
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))

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
