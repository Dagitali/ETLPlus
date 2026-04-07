"""
:mod:`tests.integration.cli.test_i_cli_check` module.

Integration-scope smoke tests for the ``etlplus check`` CLI command.
"""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING
from typing import Any

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonOutputParser
    from tests.integration.cli.conftest import PipelineConfigFactory

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestCliCheck:
    """Smoke tests for the ``etlplus check`` CLI command."""

    def test_graph_reports_topological_order(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        tmp_path: Path,
    ) -> None:
        """``check --graph`` should validate dependencies and print DAG order."""
        config_path = tmp_path / 'check_graph_ok.yml'
        config_path.write_text(
            dedent(
                """
                name: Graph Check
                sources:
                  - name: seed_src
                    type: file
                    format: json
                    path: "./seed.json"
                  - name: publish_src
                    type: file
                    format: json
                    path: "./publish.json"
                targets:
                  - name: seed_out
                    type: file
                    format: json
                    path: "./seed-out.json"
                  - name: publish_out
                    type: file
                    format: json
                    path: "./publish-out.json"
                jobs:
                  - name: publish
                    depends_on: [seed]
                    extract:
                      source: publish_src
                    load:
                      target: publish_out
                  - name: seed
                    extract:
                      source: seed_src
                    load:
                      target: seed_out
                """,
            ).strip(),
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            ('check', '--config', str(config_path), '--graph'),
        )

        assert code == 0
        assert err == ''
        payload = parse_json_output(out)
        assert payload['status'] == 'ok'
        assert payload['ordered_jobs'] == ['seed', 'publish']
        assert payload['jobs'] == [
            {'depends_on': [], 'name': 'seed'},
            {'depends_on': ['seed'], 'name': 'publish'},
        ]

    def test_graph_reports_invalid_dependency_cycles(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        tmp_path: Path,
    ) -> None:
        """``check --graph`` should fail invalid dependency graphs."""
        config_path = tmp_path / 'check_graph_invalid.yml'
        config_path.write_text(
            dedent(
                """
                name: Graph Check
                sources:
                  - name: src
                    type: file
                    format: json
                    path: "./in.json"
                targets:
                  - name: out
                    type: file
                    format: json
                    path: "./out.json"
                jobs:
                  - name: alpha
                    depends_on: [beta]
                    extract:
                      source: src
                    load:
                      target: out
                  - name: beta
                    depends_on: [alpha]
                    extract:
                      source: src
                    load:
                      target: out
                """,
            ).strip(),
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            ('check', '--config', str(config_path), '--graph'),
        )

        assert code == 1
        assert err == ''
        payload = parse_json_output(out)
        assert payload == {
            'message': 'Dependency cycle detected',
            'status': 'error',
        }

    def test_jobs_lists_job(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        pipeline_config_factory: PipelineConfigFactory,
        sample_records: list[dict[str, Any]],
    ) -> None:
        """Test that ``check --jobs`` returns the configured job name."""
        cfg = pipeline_config_factory(sample_records)
        code, out, err = cli_invoke(
            ('check', '--config', str(cfg.config_path), '--jobs'),
        )
        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert cfg.job_name in payload.get('jobs', [])

    def test_readiness_accepts_resolved_substitutions(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``check --readiness`` passes with satisfied substitutions."""
        monkeypatch.setenv('ETLPLUS_READINESS_TOKEN', 'secret-token')
        config_path = tmp_path / 'check_readiness_ok.yml'
        config_path.write_text(
            dedent(
                """
                name: Readiness Check
                profile:
                  env:
                    API_TOKEN: "${ETLPLUS_READINESS_TOKEN}"
                vars:
                  output_dir: temp
                targets:
                  - name: out
                    type: file
                    format: json
                    path: "${output_dir}/out.json"
                """,
            ).strip(),
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            ('check', '--readiness', '--config', str(config_path)),
        )
        assert code == 0
        assert err == ''
        payload = parse_json_output(out)
        assert payload['status'] == 'ok'
        substitution_check = next(
            check
            for check in payload['checks']
            if check['name'] == 'config-substitution'
        )
        assert substitution_check['status'] == 'ok'

    def test_readiness_incomplete_explicit_aws_credentials_exit_one(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Partial explicit AWS credentials should fail readiness."""
        from etlplus.runtime import ReadinessReportBuilder

        monkeypatch.setattr(
            ReadinessReportBuilder,
            'package_available',
            lambda _module_name: True,
        )
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'access-key')
        monkeypatch.delenv('AWS_SECRET_ACCESS_KEY', raising=False)
        monkeypatch.delenv('AWS_SESSION_TOKEN', raising=False)
        config_path = tmp_path / 'check_readiness_partial_aws.yml'
        config_path.write_text(
            dedent(
                """
                name: Readiness Check
                sources:
                  - name: s3_source
                    type: file
                    format: json
                    path: "s3://bucket/input.json"
                """,
            ).strip(),
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            ('check', '--readiness', '--config', str(config_path)),
        )

        assert code == 1
        assert err == ''
        payload = parse_json_output(out)
        assert payload['status'] == 'error'
        provider_check = next(
            check
            for check in payload['checks']
            if check['name'] == 'provider-environment'
        )
        assert provider_check['environment_gaps'] == [
            {
                'connector': 's3_source',
                'guidance': (
                    'Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY together, or '
                    'remove the partial explicit credential env vars and rely on '
                    'AWS_PROFILE, shared config files, container credentials, or '
                    'instance metadata.'
                ),
                'missing_env': ['AWS_SECRET_ACCESS_KEY'],
                'provider': 'aws-s3',
                'reason': (
                    'Incomplete explicit AWS access-key configuration was detected '
                    'for this S3 path.'
                ),
                'role': 'source',
                'scheme': 's3',
                'severity': 'error',
            },
        ]

    def test_readiness_rejects_inspection_flags(
        self,
        cli_invoke: CliInvoke,
    ) -> None:
        """Test that readiness mode cannot be mixed with inspection flags."""
        code, _out, err = cli_invoke(('check', '--readiness', '--jobs'))
        assert code == 2
        assert '--readiness cannot be combined with inspection flags' in err

    def test_readiness_reports_connector_gaps(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
    ) -> None:
        """Test that readiness reports connector-specific config gaps."""
        config_path = tmp_path / 'check_readiness_connector_gap.yml'
        config_path.write_text(
            dedent(
                """
                name: Readiness Check
                targets:
                  - name: warehouse
                    type: database
                """,
            ).strip(),
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            ('check', '--readiness', '--config', str(config_path)),
        )
        assert code == 1
        assert err == ''
        payload = parse_json_output(out)
        connector_check = next(
            check
            for check in payload['checks']
            if check['name'] == 'connector-readiness'
        )
        assert connector_check['status'] == 'error'
        assert connector_check['gaps'][0]['issue'] == 'missing connection_string'

    def test_readiness_reports_missing_storage_extra(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that readiness flags missing optional storage dependencies."""
        from etlplus.runtime import ReadinessReportBuilder

        original_package_available = ReadinessReportBuilder.package_available
        monkeypatch.setattr(
            ReadinessReportBuilder,
            'package_available',
            lambda module_name: (
                False
                if module_name == 'boto3'
                else original_package_available(module_name)
            ),
        )
        config_path = tmp_path / 'check_readiness_missing_storage_extra.yml'
        config_path.write_text(
            dedent(
                """
                name: Readiness Check
                targets:
                  - name: out
                    type: file
                    format: json
                    path: "s3://bucket/out.json"
                """,
            ).strip(),
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            ('check', '--readiness', '--config', str(config_path)),
        )
        assert code == 1
        assert err == ''
        payload = parse_json_output(out)
        dependency_check = next(
            check
            for check in payload['checks']
            if check['name'] == 'optional-dependencies'
        )
        assert dependency_check['status'] == 'error'
        assert dependency_check['missing_requirements'][0] == {
            'connector': 'out',
            'detected_scheme': 's3',
            'extra': 'storage',
            'guidance': (
                'Install boto3 directly or install the ETLPlus "storage" extra. '
                'Required for "s3" storage paths.'
            ),
            'missing_package': 'boto3',
            'reason': 's3 storage path requires boto3',
            'role': 'target',
        }

    def test_readiness_reports_unresolved_substitutions(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``check --readiness`` fails unresolved substitutions."""
        monkeypatch.delenv('ETLPLUS_READINESS_TOKEN', raising=False)
        config_path = tmp_path / 'check_readiness_missing_env.yml'
        config_path.write_text(
            dedent(
                """
                name: Readiness Check
                profile:
                  env:
                    API_TOKEN: "${ETLPLUS_READINESS_TOKEN}"
                vars:
                  output_dir: temp
                targets:
                  - name: out
                    type: file
                    format: json
                    path: "${output_dir}/out.json"
                """,
            ).strip(),
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            ('check', '--readiness', '--config', str(config_path)),
        )
        assert code == 1
        assert err == ''
        payload = parse_json_output(out)
        assert payload['status'] == 'error'
        substitution_check = next(
            check
            for check in payload['checks']
            if check['name'] == 'config-substitution'
        )
        assert substitution_check['status'] == 'error'
        assert substitution_check['missing_env'] == ['ETLPLUS_READINESS_TOKEN']
        assert substitution_check['references'] == [
            {
                'name': 'ETLPLUS_READINESS_TOKEN',
                'paths': ['profile.env.API_TOKEN'],
            },
        ]
        assert 'ETLPLUS_READINESS_TOKEN' in substitution_check['unresolved_tokens']

    def test_readiness_provider_error_exits_one(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Provider bootstrap errors should keep readiness exit code non-zero."""
        from etlplus.runtime import ReadinessReportBuilder

        monkeypatch.setattr(
            ReadinessReportBuilder,
            'package_available',
            lambda _module_name: True,
        )
        config_path = tmp_path / 'check_readiness_provider_error.yml'
        config_path.write_text(
            dedent(
                """
                name: Readiness Check
                targets:
                  - name: blob_target
                    type: file
                    format: json
                    path: "azure-blob://container/out.json"
                """,
            ).strip(),
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            ('check', '--readiness', '--config', str(config_path)),
        )

        assert code == 1
        assert err == ''
        payload = parse_json_output(out)
        assert payload['status'] == 'error'
        provider_check = next(
            check
            for check in payload['checks']
            if check['name'] == 'provider-environment'
        )
        assert provider_check['status'] == 'error'
        assert provider_check['environment_gaps'] == [
            {
                'connector': 'blob_target',
                'guidance': (
                    'Set AZURE_STORAGE_CONNECTION_STRING, set '
                    'AZURE_STORAGE_ACCOUNT_URL, or include the account host in '
                    'the path authority.'
                ),
                'missing_env': [
                    'AZURE_STORAGE_CONNECTION_STRING',
                    'AZURE_STORAGE_ACCOUNT_URL',
                ],
                'provider': 'azure-storage',
                'reason': (
                    'azure-blob path does not provide an account host and no '
                    'Azure storage bootstrap settings were found.'
                ),
                'role': 'target',
                'scheme': 'azure-blob',
                'severity': 'error',
            },
        ]

    def test_readiness_provider_warning_exits_zero(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Provider warnings should remain advisory and keep exit code zero."""
        from etlplus.runtime import ReadinessReportBuilder

        monkeypatch.setattr(
            ReadinessReportBuilder,
            'package_available',
            lambda _module_name: True,
        )
        config_path = tmp_path / 'check_readiness_provider_warning.yml'
        config_path.write_text(
            dedent(
                """
                name: Readiness Check
                sources:
                  - name: s3_source
                    type: file
                    format: json
                    path: "s3://bucket/input.json"
                """,
            ).strip(),
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            ('check', '--readiness', '--config', str(config_path)),
        )

        assert code == 0
        assert err == ''
        payload = parse_json_output(out)
        assert payload['status'] == 'warn'
        provider_check = next(
            check
            for check in payload['checks']
            if check['name'] == 'provider-environment'
        )
        assert provider_check['status'] == 'warn'
        assert provider_check['environment_gaps'] == [
            {
                'connector': 's3_source',
                'guidance': (
                    'Set AWS_PROFILE or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, '
                    'or rely on shared config files, container credentials, or '
                    'instance metadata.'
                ),
                'missing_env': [
                    'AWS_ACCESS_KEY_ID',
                    'AWS_SECRET_ACCESS_KEY',
                    'AWS_SESSION_TOKEN',
                    'AWS_PROFILE',
                    'AWS_DEFAULT_PROFILE',
                    'AWS_ROLE_ARN',
                    'AWS_WEB_IDENTITY_TOKEN_FILE',
                    'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI',
                    'AWS_CONTAINER_CREDENTIALS_FULL_URI',
                    'AWS_SHARED_CREDENTIALS_FILE',
                    'AWS_CONFIG_FILE',
                ],
                'provider': 'aws-s3',
                'reason': (
                    'No common AWS credential-chain environment hints were '
                    'detected for this S3 path.'
                ),
                'role': 'source',
                'scheme': 's3',
                'severity': 'warn',
            },
        ]

    def test_readiness_runtime_only(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
    ) -> None:
        """Test that ``check --readiness`` succeeds without a config file."""
        code, out, err = cli_invoke(('check', '--readiness'))
        assert code == 0
        assert err == ''
        payload = parse_json_output(out)
        assert payload['status'] == 'ok'
        assert any(check['name'] == 'python-version' for check in payload['checks'])

    def test_readiness_strict_reports_malformed_entries(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
    ) -> None:
        """Strict readiness mode should surface malformed skipped config entries."""
        config_path = tmp_path / 'check_readiness_strict.yml'
        config_path.write_text(
            dedent(
                """
                name: Strict Readiness Check
                sources:
                  - just-a-string
                targets:
                  - name: out
                    type: file
                    format: json
                    path: "./temp/out.json"
                jobs:
                  - name: publish
                    extract:
                      source: missing-source
                    load:
                      target: out
                """,
            ).strip(),
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            ('check', '--readiness', '--strict', '--config', str(config_path)),
        )

        assert code == 1
        assert err == ''
        payload = parse_json_output(out)
        structure_check = next(
            check for check in payload['checks'] if check['name'] == 'config-structure'
        )
        assert structure_check['status'] == 'error'
        assert any(
            issue['issue'] == 'invalid connector entry'
            for issue in structure_check['issues']
        )
        assert any(
            issue['issue'] == 'unknown source reference: missing-source'
            for issue in structure_check['issues']
        )
