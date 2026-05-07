"""
:mod:`tests.integration.cli.test_i_cli_execution_events` module.

Integration coverage for structured execution events across CLI commands.
"""

from __future__ import annotations

import io
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING
from typing import Any

import pytest

from etlplus import __version__
from etlplus.runtime import EVENT_SCHEMA
from etlplus.runtime import EVENT_SCHEMA_VERSION

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonOutputParser

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]


# SECTION: HELPERS ========================================================== #


EXPECTED_BASE_EVENT_FIELDS = {
    'command',
    'event',
    'lifecycle',
    'run_id',
    'schema',
    'schema_version',
    'timestamp',
}


def _assert_base_event_fields(
    line: dict[str, Any],
    *,
    command: str,
    lifecycle: str,
) -> None:
    """Assert one emitted event keeps the stable base envelope."""
    assert EXPECTED_BASE_EVENT_FIELDS.issubset(line)
    assert line['command'] == command
    assert line['event'] == f'{command}.{lifecycle}'
    assert line['lifecycle'] == lifecycle
    assert line['schema'] == EVENT_SCHEMA
    assert line['schema_version'] == EVENT_SCHEMA_VERSION
    assert isinstance(line['run_id'], str)
    assert datetime.fromisoformat(line['timestamp'])


def _parse_event_lines(stderr: str) -> list[dict[str, Any]]:
    """Parse JSONL event output from STDERR."""
    return [
        json.loads(line) for line in stderr.splitlines() if line.strip().startswith('{')
    ]


def _assert_success_lifecycle(
    lines: list[dict[str, Any]],
    *,
    command: str,
    command_fields: dict[str, object],
) -> None:
    """Assert a started/completed event pair for one successful invocation."""
    assert [line['event'] for line in lines] == [
        f'{command}.started',
        f'{command}.completed',
    ]
    for line, lifecycle in zip(lines, ('started', 'completed'), strict=True):
        _assert_base_event_fields(line, command=command, lifecycle=lifecycle)
        for key, value in command_fields.items():
            assert line[key] == value

    assert lines[0]['run_id'] == lines[1]['run_id']
    assert isinstance(lines[1]['duration_ms'], int)
    assert lines[1]['status'] == 'ok'


def _assert_failed_lifecycle(
    lines: list[dict[str, Any]],
    *,
    command: str,
    command_fields: dict[str, object],
    error_type: str,
    error_message_contains: str,
) -> None:
    """Assert a started/failed event pair for one failed invocation."""
    assert [line['event'] for line in lines] == [
        f'{command}.started',
        f'{command}.failed',
    ]
    for line, lifecycle in zip(lines, ('started', 'failed'), strict=True):
        _assert_base_event_fields(line, command=command, lifecycle=lifecycle)
        for key, value in command_fields.items():
            assert line[key] == value

    assert lines[0]['run_id'] == lines[1]['run_id']
    assert isinstance(lines[1]['duration_ms'], int)
    assert lines[1]['status'] == 'error'
    assert lines[1]['error_type'] == error_type
    assert error_message_contains in lines[1]['error_message']


def _history_run_row(
    history_db: Path,
    run_id: str,
) -> tuple[str, str | None, str | None] | None:
    """Return the persisted run status plus error metadata for one run ID."""
    with sqlite3.connect(history_db) as conn:
        return conn.execute(
            """
            SELECT status, error_type, error_message
            FROM runs
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()


def _write_handled_dag_failure_config(
    tmp_path: Path,
) -> Path:
    """Write one DAG config that fails in-job and returns a handled run summary."""
    missing_source_path = tmp_path / 'missing-source.json'
    missing_intermediate_path = tmp_path / 'missing-intermediate.json'
    intermediate_path = tmp_path / 'intermediate.json'
    final_path = tmp_path / 'final.json'
    config_path = tmp_path / 'run-events-dag-failure.yml'
    config_path.write_text(
        dedent(
            f"""
            name: DAG Failure Events
            sources:
              - name: source_in
                type: file
                format: json
                path: "{missing_source_path}"
              - name: intermediate_in
                type: file
                format: json
                path: "{missing_intermediate_path}"
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
    return config_path


def _write_exception_run_config(
    tmp_path: Path,
) -> Path:
    """Write one run config that triggers an exception-driven failure after start."""
    target_path = tmp_path / 'exception-out.json'
    config_path = tmp_path / 'run-events-exception.yml'
    config_path.write_text(
        dedent(
            f"""
            name: Exception Failure Events
            sources:
              - name: source_in
                type: file
                format: json
            targets:
              - name: target_out
                type: file
                format: json
                path: "{target_path}"
            jobs:
              - name: broken_job
                extract:
                  source: source_in
                load:
                  target: target_out
            """,
        ).strip(),
        encoding='utf-8',
    )
    return config_path


# SECTION: TESTS ============================================================ #


class TestCliExecutionEvents:
    """Structured event coverage for execution-oriented commands."""

    def test_extract_emits_jsonl_events(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        json_payload_file: Path,
        sample_records: list[dict[str, Any]],
    ) -> None:
        """Test that ``extract --event-format jsonl`` emits stable events."""
        code, out, err = cli_invoke(
            ('extract', '--event-format', 'jsonl', str(json_payload_file)),
        )

        assert code == 0
        assert parse_json_output(out) == sample_records
        _assert_success_lifecycle(
            _parse_event_lines(err),
            command='extract',
            command_fields={
                'source': str(json_payload_file),
                'source_type': 'file',
            },
        )

    def test_load_emits_jsonl_events(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        sample_records_json: str,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Test that ``load --event-format jsonl`` emits stable events."""
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))
        out_path = tmp_path / 'load-events.json'

        code, out, err = cli_invoke(
            (
                'load',
                '--event-format',
                'jsonl',
                '--target-type',
                'file',
                str(out_path),
            ),
        )

        assert code == 0
        assert parse_json_output(out)['status'] == 'success'
        _assert_success_lifecycle(
            _parse_event_lines(err),
            command='load',
            command_fields={
                'source': '-',
                'target': str(out_path),
                'target_type': 'file',
            },
        )

    def test_transform_emits_jsonl_events(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        operations_json: str,
        sample_records_json: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``transform --event-format jsonl`` emits stable events."""
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))

        code, out, err = cli_invoke(
            (
                'transform',
                '--event-format',
                'jsonl',
                '--operations',
                operations_json,
                '-',
                '-',
            ),
        )

        assert code == 0
        assert isinstance(parse_json_output(out), list)
        _assert_success_lifecycle(
            _parse_event_lines(err),
            command='transform',
            command_fields={
                'source': '-',
                'target': 'stdout',
                'target_type': 'file',
            },
        )

    def test_validate_emits_jsonl_events(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        rules_json: str,
        sample_records_json: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``validate --event-format jsonl`` emits stable events."""
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))

        code, out, err = cli_invoke(
            (
                'validate',
                '--event-format',
                'jsonl',
                '--rules',
                rules_json,
                '-',
            ),
        )

        assert code == 0
        assert parse_json_output(out)['valid'] is True
        _assert_success_lifecycle(
            _parse_event_lines(err),
            command='validate',
            command_fields={
                'source': '-',
                'target': 'stdout',
            },
        )

    def test_run_emits_jsonl_events_and_correlates_with_history_on_success(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        pipeline_config_factory,
        sample_records: list[dict[str, Any]],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that successful runs correlate event, stdout, and persisted
        history run IDs.
        """
        cfg = pipeline_config_factory(sample_records)
        state_dir = tmp_path / 'state'
        monkeypatch.setenv('ETLPLUS_STATE_DIR', str(state_dir))

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
        lines = _parse_event_lines(err)

        _assert_success_lifecycle(
            lines,
            command='run',
            command_fields={
                'config_path': str(cfg.config_path),
                'continue_on_fail': False,
                'etlplus_version': __version__,
                'job': cfg.job_name,
                'pipeline_name': 'Smoke Test',
                'run_all': False,
            },
        )
        assert payload['run_id'] == lines[0]['run_id']
        assert _history_run_row(
            state_dir / 'history.sqlite',
            payload['run_id'],
        ) == ('succeeded', None, None)

    def test_run_emits_failed_jsonl_events_for_handled_dag_failure(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that handled DAG summary failures emit the stable `run.failed`
        contract.
        """
        config_path = _write_handled_dag_failure_config(tmp_path)
        state_dir = tmp_path / 'state'
        monkeypatch.setenv('ETLPLUS_STATE_DIR', str(state_dir))

        code, out, err = cli_invoke(
            (
                'run',
                '--config',
                str(config_path),
                '--all',
                '--continue-on-fail',
                '--event-format',
                'jsonl',
            ),
        )

        assert code == 1
        payload = parse_json_output(out)
        lines = _parse_event_lines(err)

        _assert_failed_lifecycle(
            lines,
            command='run',
            command_fields={
                'config_path': str(config_path),
                'continue_on_fail': True,
                'etlplus_version': __version__,
                'job': None,
                'pipeline_name': 'DAG Failure Events',
                'run_all': True,
            },
            error_type='RunExecutionFailed',
            error_message_contains='DAG execution',
        )
        assert payload['run_id'] == lines[0]['run_id']
        assert payload['status'] == 'error'
        assert _history_run_row(
            state_dir / 'history.sqlite',
            payload['run_id'],
        ) == (
            'failed',
            'RunExecutionFailed',
            'Job "seed" failed during DAG execution',
        )
        with sqlite3.connect(state_dir / 'history.sqlite') as conn:
            job_rows = conn.execute(
                """
                SELECT run_id, job_name, status
                FROM job_runs
                WHERE run_id = ?
                ORDER BY sequence_index
                """,
                (payload['run_id'],),
            ).fetchall()
        assert job_rows == [
            (payload['run_id'], 'seed', 'failed'),
            (payload['run_id'], 'publish', 'skipped'),
        ]

    def test_run_emits_failed_jsonl_events_for_exception_failure(
        self,
        cli_invoke: CliInvoke,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that exception-driven run failures reuse the same stable failed
        envelope.
        """
        config_path = _write_exception_run_config(tmp_path)
        state_dir = tmp_path / 'state'
        monkeypatch.setenv('ETLPLUS_STATE_DIR', str(state_dir))

        code, out, err = cli_invoke(
            (
                'run',
                '--config',
                str(config_path),
                '--job',
                'broken_job',
                '--event-format',
                'jsonl',
            ),
        )

        assert code == 1
        assert out == ''
        lines = _parse_event_lines(err)

        _assert_failed_lifecycle(
            lines,
            command='run',
            command_fields={
                'config_path': str(config_path),
                'continue_on_fail': False,
                'etlplus_version': __version__,
                'job': 'broken_job',
                'pipeline_name': 'Exception Failure Events',
                'run_all': False,
            },
            error_type='ValueError',
            error_message_contains='File source missing "path"',
        )
        run_id = lines[0]['run_id']
        assert _history_run_row(
            state_dir / 'history.sqlite',
            run_id,
        ) == (
            'failed',
            'ValueError',
            'File source missing "path"',
        )
