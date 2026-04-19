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
import importlib
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

# SECTION: HELPERS ========================================================== #


def _write_dag_pipeline_config(
    tmp_path: Path,
    sample_records: list[dict[str, object]],
) -> tuple[Path, Path]:
    """Materialize one two-job DAG pipeline config and return config/output paths."""
    source_path = tmp_path / 'source.json'
    intermediate_path = tmp_path / 'intermediate.json'
    final_path = tmp_path / 'final.json'
    File(source_path, FileFormat.JSON).write(sample_records)
    config_path = tmp_path / 'pipeline_all.yml'
    config_path.write_text(
        dedent(
            f"""
            name: DAG Smoke Test
            sources:
              - name: source_in
                type: file
                format: json
                path: "{source_path}"
              - name: intermediate_in
                type: file
                format: json
                path: "{intermediate_path}"
            targets:
              - name: intermediate_out
                type: file
                format: json
                path: "{intermediate_path}"
              - name: final_out
                type: file
                format: json
                path: "{final_path}"
            jobs:
              - name: publish
                depends_on: [seed]
                extract:
                  source: intermediate_in
                load:
                  target: final_out
              - name: seed
                extract:
                  source: source_in
                load:
                  target: intermediate_out
            """,
        ).strip(),
        encoding='utf-8',
    )
    return config_path, final_path


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
        """
        Test that CLI run can write a local file source to a real cloud target.
        """
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

    def test_run_all_accepts_max_concurrency_override(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        sample_records: list[dict[str, object]],
        tmp_path: Path,
    ) -> None:
        """The run command should accept an explicit bounded-concurrency override."""
        config_path, final_path = _write_dag_pipeline_config(tmp_path, sample_records)

        code, out, err = cli_invoke(
            ('run', '--config', str(config_path), '--all', '--max-concurrency', '2'),
        )

        assert code == 0
        assert err == ''
        payload = parse_json_output(out)
        assert payload['status'] == 'ok'
        assert payload['result']['status'] == 'success'
        assert payload['result']['max_concurrency'] == 2
        assert File(final_path, FileFormat.JSON).read() == sample_records

    def test_run_all_executes_dependency_order_and_returns_summary(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        sample_records: list[dict[str, object]],
        tmp_path: Path,
    ) -> None:
        """
        Test that the command ``run --all`` executes jobs in DAG order and emit
        a summary.
        """
        config_path, final_path = _write_dag_pipeline_config(tmp_path, sample_records)

        code, out, err = cli_invoke(
            ('run', '--config', str(config_path), '--all'),
        )

        assert code == 0
        assert err == ''
        payload = parse_json_output(out)
        assert payload['status'] == 'ok'
        assert payload['result']['status'] == 'success'
        assert payload['result']['mode'] == 'all'
        assert payload['result']['ordered_jobs'] == ['seed', 'publish']
        assert [job['job'] for job in payload['result']['executed_jobs']] == [
            'seed',
            'publish',
        ]
        assert File(final_path, FileFormat.JSON).read() == sample_records

    def test_run_all_persists_compact_run_summary_and_job_history(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        sample_records: list[dict[str, object]],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that DAG runs persists one aggregate run row plus per-job history
        rows.
        """
        config_path, final_path = _write_dag_pipeline_config(tmp_path, sample_records)
        state_dir = tmp_path / 'state'
        monkeypatch.setenv('ETLPLUS_STATE_DIR', str(state_dir))

        code, out, err = cli_invoke(
            ('run', '--config', str(config_path), '--all'),
        )

        assert code == 0
        assert err == ''
        payload = parse_json_output(out)
        run_id = payload['run_id']
        expected_summary = {
            'continue_on_fail': False,
            'executed_job_count': 2,
            'failed_job_count': 0,
            'failed_jobs': [],
            'final_job': 'publish',
            'final_result_status': 'success',
            'job_count': 2,
            'mode': 'all',
            'ordered_jobs': ['seed', 'publish'],
            'requested_job': None,
            'skipped_job_count': 0,
            'skipped_jobs': [],
            'status': 'success',
            'succeeded_job_count': 2,
            'succeeded_jobs': ['seed', 'publish'],
        }

        history_db = state_dir / 'history.sqlite'
        with sqlite3.connect(history_db) as conn:
            run_row = conn.execute(
                """
                SELECT job_name, result_summary
                FROM runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            job_rows = conn.execute(
                """
                SELECT run_id, job_name, sequence_index, status, duration_ms
                FROM job_runs
                WHERE run_id = ?
                ORDER BY sequence_index
                """,
                (run_id,),
            ).fetchall()

        assert run_row is not None
        assert run_row[0] is None
        assert json.loads(run_row[1]) == expected_summary
        assert [(row[0], row[1], row[2], row[3]) for row in job_rows] == [
            (run_id, 'seed', 0, 'succeeded'),
            (run_id, 'publish', 1, 'succeeded'),
        ]
        assert all(isinstance(row[4], int) for row in job_rows)
        assert File(final_path, FileFormat.JSON).read() == sample_records

        history_code, history_out, history_err = cli_invoke(
            ('history', '--run-id', run_id),
        )
        assert history_code == 0
        assert history_err == ''
        history_payload = parse_json_output(history_out)
        assert len(history_payload) == 1
        assert history_payload[0]['run_id'] == run_id
        assert history_payload[0]['job_name'] is None
        assert history_payload[0]['result_summary'] == expected_summary
        assert 'executed_jobs' not in history_payload[0]['result_summary']

        job_history_code, job_history_out, job_history_err = cli_invoke(
            ('history', '--level', 'job', '--run-id', run_id),
        )
        assert job_history_code == 0
        assert job_history_err == ''
        job_history_payload = sorted(
            parse_json_output(job_history_out),
            key=lambda row: row['sequence_index'],
        )
        assert [
            (row['run_id'], row['job_name'], row['sequence_index'], row['status'])
            for row in job_history_payload
        ] == [
            (run_id, 'seed', 0, 'succeeded'),
            (run_id, 'publish', 1, 'succeeded'),
        ]
        assert all(isinstance(row['duration_ms'], int) for row in job_history_payload)

        status_code, status_out, status_err = cli_invoke(
            ('status', '--run-id', run_id),
        )
        assert status_code == 0
        assert status_err == ''
        status_payload = parse_json_output(status_out)
        assert status_payload['run_id'] == run_id
        assert status_payload['result_summary'] == expected_summary

        report_code, report_out, report_err = cli_invoke(
            ('report', '--level', 'job', '--run-id', run_id, '--group-by', 'status'),
        )
        assert report_code == 0
        assert report_err == ''
        report_payload = parse_json_output(report_out)
        assert report_payload['group_by'] == 'status'
        assert report_payload['summary']['runs'] == 2
        assert report_payload['summary']['succeeded'] == 2
        assert len(report_payload['rows']) == 1
        assert report_payload['rows'][0]['group'] == 'succeeded'
        assert report_payload['rows'][0]['runs'] == 2
        assert report_payload['rows'][0]['success_rate_pct'] == 100.0

    def test_run_all_persists_retry_metadata_in_run_and_job_history(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        sample_records: list[dict[str, object]],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """DAG retries should persist additive metadata in run and job history."""
        run_mod = importlib.import_module('etlplus.ops.run')

        config_path, final_path = _write_dag_pipeline_config(tmp_path, sample_records)
        state_dir = tmp_path / 'state'
        monkeypatch.setenv('ETLPLUS_STATE_DIR', str(state_dir))

        original_run_job_config = run_mod._run_job_config
        attempts = {'seed': 0}

        def _flaky_run_job_config(
            context: object,
            job_obj: object,
        ) -> dict[str, object]:
            if getattr(job_obj, 'name', None) == 'seed':
                attempts['seed'] += 1
                if attempts['seed'] == 1:
                    raise ValueError('temporary seed failure')
            return original_run_job_config(context, job_obj)

        monkeypatch.setattr(run_mod, '_run_job_config', _flaky_run_job_config)

        source_path = tmp_path / 'source.json'
        intermediate_path = tmp_path / 'intermediate.json'
        config_path.write_text(
            dedent(
                f"""
                name: DAG Smoke Test
                sources:
                  - name: source_in
                    type: file
                    format: json
                    path: "{source_path}"
                  - name: intermediate_in
                    type: file
                    format: json
                    path: "{intermediate_path}"
                targets:
                  - name: intermediate_out
                    type: file
                    format: json
                    path: "{intermediate_path}"
                  - name: final_out
                    type: file
                    format: json
                    path: "{final_path}"
                jobs:
                  - name: publish
                    depends_on: [seed]
                    extract:
                      source: intermediate_in
                    load:
                      target: final_out
                  - name: seed
                    retry:
                      max_attempts: 3
                      backoff_seconds: 0.0
                    extract:
                      source: source_in
                    load:
                      target: intermediate_out
                """,
            ).strip(),
            encoding='utf-8',
        )
        code, out, err = cli_invoke(
            ('run', '--config', str(config_path), '--all'),
        )

        assert code == 0
        assert err == ''
        payload = parse_json_output(out)
        run_id = payload['run_id']
        assert payload['result']['retried_job_count'] == 1
        assert payload['result']['retried_jobs'] == ['seed']
        assert payload['result']['total_retry_count'] == 1
        assert payload['result']['total_attempt_count'] == 3

        history_db = state_dir / 'history.sqlite'
        with sqlite3.connect(history_db) as conn:
            run_row = conn.execute(
                """
                SELECT result_summary
                FROM runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            job_rows = conn.execute(
                """
                SELECT job_name, result_summary
                FROM job_runs
                WHERE run_id = ?
                ORDER BY sequence_index
                """,
                (run_id,),
            ).fetchall()

        assert run_row is not None
        persisted_run_summary = json.loads(run_row[0])
        assert persisted_run_summary['retried_job_count'] == 1
        assert persisted_run_summary['retried_jobs'] == ['seed']
        assert persisted_run_summary['total_retry_count'] == 1
        assert persisted_run_summary['total_attempt_count'] == 3

        persisted_job_summaries = {
            row[0]: json.loads(row[1]) if row[1] is not None else None
            for row in job_rows
        }
        seed_summary = persisted_job_summaries['seed']
        publish_summary = persisted_job_summaries['publish']
        assert isinstance(seed_summary, dict)
        assert isinstance(publish_summary, dict)
        assert seed_summary['retry']['attempt_count'] == 2
        assert seed_summary['retry']['max_attempts'] == 3
        assert seed_summary['retry']['retried'] is True
        assert [item['status'] for item in seed_summary['retry']['attempts']] == [
            'failed',
            'succeeded',
        ]
        assert publish_summary['status'] == 'success'
        assert File(final_path, FileFormat.JSON).read() == sample_records

    def test_run_captures_traceback_when_enabled_in_config(
        self,
        cli_invoke: CliInvoke,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A failing run should persist a traceback when config capture is enabled."""
        monkeypatch.delenv('ETLPLUS_STATE_DIR', raising=False)
        source_path = tmp_path / 'input.json'
        File(source_path, FileFormat.JSON).write([{'id': 1}])
        state_dir = tmp_path / 'history-state'
        config_path = tmp_path / 'traceback_pipeline.yml'
        config_path.write_text(
            dedent(
                f"""
                name: Traceback Smoke Test
                history:
                  state_dir: "{state_dir}"
                  capture_tracebacks: true
                sources:
                  - name: source_in
                    type: file
                    format: json
                    path: "{source_path}"
                targets:
                  - name: output_out
                    type: file
                    format: json
                    path: "{tmp_path / 'output.json'}"
                jobs:
                  - name: seed
                    extract:
                      source: source_in
                    load:
                      target: output_out
                """,
            ).strip(),
            encoding='utf-8',
        )

        code, _out, err = cli_invoke(
            (
                'run',
                '--config',
                str(config_path),
                '--job',
                'missing_job',
            ),
        )

        assert code == 1
        assert 'Job not found: missing_job' in err
        history_db = state_dir / 'history.sqlite'
        assert history_db.exists()

        with sqlite3.connect(history_db) as conn:
            row = conn.execute(
                """
                SELECT status, error_type, error_message, error_traceback
                FROM runs
                ORDER BY started_at DESC
                LIMIT 1
                """,
            ).fetchone()

        assert row is not None
        assert row[0] == 'failed'
        assert row[1] == 'ValueError'
        assert 'Job not found: missing_job' in row[2]
        assert row[3] is not None
        assert 'ValueError: Job not found: missing_job' in row[3]

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

    def test_run_supports_history_backend_and_state_dir_overrides(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        pipeline_config_factory: PipelineConfigFactory,
        sample_records: list[dict[str, object]],
        tmp_path: Path,
    ) -> None:
        """Run history overrides should route persistence to the requested backend."""
        cfg = pipeline_config_factory(sample_records)
        history_state_dir = tmp_path / 'history-jsonl'

        code, out, err = cli_invoke(
            (
                'run',
                '--config',
                str(cfg.config_path),
                '--job',
                cfg.job_name,
                '--history-backend',
                'jsonl',
                '--history-state-dir',
                str(history_state_dir),
            ),
        )

        assert code == 0
        assert err == ''
        payload = parse_json_output(out)
        history_log = history_state_dir / 'history.jsonl'
        assert history_log.exists()
        assert not (history_state_dir / 'history.sqlite').exists()

        records = [
            json.loads(line)
            for line in history_log.read_text(encoding='utf-8').splitlines()
            if line.strip()
        ]
        assert [record['record_level'] for record in records] == ['run', 'run']
        assert all(record['run_id'] == payload['run_id'] for record in records)
