"""
:mod:`tests.integration.cli.test_i_cli_run` module.

Integration-scope smoke test suite for a minimal file→file CLI job.

Notes
-----
- Builds a transient pipeline YAML string per test run.
- Invokes ``etlplus run --job <job>`` end-to-end.
- Validates output file contents against input data shape.
"""

from __future__ import annotations

import csv
import json
import sqlite3
from io import StringIO
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest

from etlplus.file import File
from etlplus.file import FileFormat
from etlplus.runtime import EVENT_SCHEMA
from etlplus.runtime import EVENT_SCHEMA_VERSION

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonFileParser
    from tests.conftest import JsonOutputParser
    from tests.integration.cli.conftest import PipelineConfigFactory
    from tests.integration.cli.conftest import RealRemoteTargetFactory
    from tests.integration.cli.conftest import RemoteStorageHarness

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]

# SECTION: TESTS ============================================================ #


class TestRun:
    """Smoke tests for file→file job via CLI."""

    def test_file_to_file(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        parse_json_file: JsonFileParser,
        pipeline_config_factory: PipelineConfigFactory,
        sample_records: list[dict[str, object]],
    ) -> None:
        """Test that file→file jobs run via CLI for multiple input datasets."""
        cfg = pipeline_config_factory(sample_records)

        code, out, err = cli_invoke(
            (
                'run',
                '--config',
                str(cfg.config_path),
                '--job',
                cfg.job_name,
            ),
        )
        assert err == ''
        assert code == 0

        payload = parse_json_output(out)

        # CLI should have printed a JSON object with status ok.
        assert payload.get('status') == 'ok'
        assert isinstance(payload.get('run_id'), str)
        assert isinstance(payload.get('result'), dict)
        assert payload['result'].get('status') == 'success'

        # Output file should exist and match input data.
        assert cfg.output_path.exists()
        out_data = parse_json_file(cfg.output_path)
        assert out_data == sample_records

    def test_remote_file_to_remote_file(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        remote_storage_harness: RemoteStorageHarness,
        tmp_path: Path,
    ) -> None:
        """Test that remote file jobs run via CLI with file options."""
        source_uri = 's3://bucket/input.csv'
        target_uri = 's3://bucket/output.csv'
        remote_storage_harness.set_text(
            source_uri,
            'name|age\nAda|36\nGrace|47\n',
        )
        job_name = 'remote_file_to_file_smoke'
        cfg_path = tmp_path / 'remote_pipeline.yml'
        cfg_path.write_text(
            dedent(
                f'''
                name: Remote Smoke Test
                sources:
                  - name: src
                    type: file
                    format: csv
                    path: "{source_uri}"
                    options:
                      delimiter: ","
                      encoding: "utf-8"
                targets:
                  - name: dest
                    type: file
                    format: csv
                    path: "{target_uri}"
                    options:
                      delimiter: ","
                      encoding: "utf-8"
                jobs:
                  - name: {job_name}
                    extract:
                      source: src
                      options:
                        delimiter: "|"
                    load:
                      target: dest
                      overrides:
                        delimiter: ";"
                ''',
            ).strip(),
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            ('run', '--config', str(cfg_path), '--job', job_name),
        )

        assert err == ''
        assert code == 0
        payload = parse_json_output(out)
        assert payload.get('status') == 'ok'
        assert isinstance(payload.get('run_id'), str)
        assert isinstance(payload.get('result'), dict)
        assert payload['result'].get('status') == 'success'
        assert payload['result'].get('message') == f'Data loaded to {target_uri}'
        rows = list(
            csv.DictReader(
                StringIO(remote_storage_harness.read_text(target_uri)),
                delimiter=';',
            ),
        )
        assert rows == [
            {'age': '36', 'name': 'Ada'},
            {'age': '47', 'name': 'Grace'},
        ]

    @pytest.mark.parametrize(
        ('env_name', 'backend_label'),
        [
            ('ETLPLUS_TEST_S3_URI', 's3'),
            ('ETLPLUS_TEST_AZURE_BLOB_URI', 'azure-blob'),
        ],
        ids=['s3', 'azure-blob'],
    )
    def test_file_to_real_remote_target(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        json_file_factory,
        sample_records: list[dict[str, object]],
        real_remote_target_factory: RealRemoteTargetFactory,
        tmp_path: Path,
        env_name: str,
        backend_label: str,
    ) -> None:
        """Test that CLI run can write a local file source to a real cloud target."""
        del backend_label
        source_path = json_file_factory(
            sample_records,
            filename='real-run-input.json',
        )
        target = real_remote_target_factory(env_name, suffix='run-real')
        job_name = 'file_to_real_remote_smoke'
        cfg_path = tmp_path / 'real_remote_pipeline.yml'
        cfg_path.write_text(
            dedent(
                f'''
                name: Real Remote Smoke Test
                sources:
                  - name: src
                    type: file
                    format: json
                    path: "{source_path}"
                targets:
                  - name: dest
                    type: file
                    format: json
                    path: "{target.uri}"
                jobs:
                  - name: {job_name}
                    extract:
                      source: src
                    load:
                      target: dest
                ''',
            ).strip(),
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            ('run', '--config', str(cfg_path), '--job', job_name),
        )

        assert err == ''
        assert code == 0
        payload = parse_json_output(out)
        assert payload.get('status') == 'ok'
        assert isinstance(payload.get('run_id'), str)
        assert isinstance(payload.get('result'), dict)
        assert payload['result'].get('status') == 'success'
        assert payload['result'].get('message') == f'Data loaded to {target.uri}'

    def test_run_emits_jsonl_events_to_stderr(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        pipeline_config_factory: PipelineConfigFactory,
        sample_records: list[dict[str, object]],
    ) -> None:
        """Test that ``run --event-format jsonl`` emits structured events."""
        cfg = pipeline_config_factory(sample_records)

        code, out, err = cli_invoke(
            (
                'run',
                '--config',
                str(cfg.config_path),
                '--job',
                cfg.job_name,
                '--event-format',
                'jsonl',
            ),
        )

        assert code == 0
        payload = parse_json_output(out)
        assert payload.get('status') == 'ok'
        assert isinstance(payload.get('run_id'), str)

        lines = [json.loads(line) for line in err.splitlines() if line.strip()]
        assert [line['event'] for line in lines] == ['run.started', 'run.completed']
        assert all(line['run_id'] == payload['run_id'] for line in lines)
        assert all(line['job'] == cfg.job_name for line in lines)
        assert all(line['schema'] == EVENT_SCHEMA for line in lines)
        assert all(line['schema_version'] == EVENT_SCHEMA_VERSION for line in lines)
        assert all(line['command'] == 'run' for line in lines)
        assert all(line['lifecycle'] in {'started', 'completed'} for line in lines)
        assert File(cfg.output_path, FileFormat.JSON).read() == sample_records

    def test_run_persists_history_record(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        pipeline_config_factory: PipelineConfigFactory,
        sample_records: list[dict[str, object]],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``run`` persists its stable run record locally."""
        cfg = pipeline_config_factory(sample_records)
        monkeypatch.setenv('ETLPLUS_STATE_DIR', str(tmp_path / 'state'))

        code, out, err = cli_invoke(
            (
                'run',
                '--config',
                str(cfg.config_path),
                '--job',
                cfg.job_name,
            ),
        )

        assert code == 0
        assert err == ''
        payload = parse_json_output(out)
        history_db = tmp_path / 'state' / 'history.sqlite'
        assert history_db.exists()

        with sqlite3.connect(history_db) as conn:
            row = conn.execute(
                """
                SELECT
                    run_id,
                    pipeline_name,
                    job_name,
                    config_path,
                    status,
                    duration_ms,
                    result_summary
                FROM runs
                WHERE run_id = ?
                """,
                (payload['run_id'],),
            ).fetchone()

        assert row is not None
        assert row[0] == payload['run_id']
        assert row[1] == 'Smoke Test'
        assert row[2] == cfg.job_name
        assert row[3] == str(cfg.config_path)
        assert row[4] == 'succeeded'
        assert isinstance(row[5], int)
        assert row[6] is not None
