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
        from etlplus.runtime import _readiness as readiness_mod

        original_package_available = (
            readiness_mod.ReadinessReportBuilder.package_available
        )
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
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
        assert dependency_check['missing_requirements'][0]['missing_package'] == 'boto3'

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
        assert 'ETLPLUS_READINESS_TOKEN' in substitution_check['unresolved_tokens']

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
