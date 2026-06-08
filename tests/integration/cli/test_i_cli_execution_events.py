"""
:mod:`tests.integration.cli.test_i_cli_execution_events` module.

Integration coverage for structured execution events across CLI commands.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING
from typing import Any

import pytest

from etlplus import __version__
from etlplus.runtime import EVENT_SCHEMA
from etlplus.runtime import EVENT_SCHEMA_VERSION
from tests.integration.cli.pytest_cli_integration_support import parse_jsonl_event_lines
from tests.pytest_shared_support import STRUCTURED_EVENT_BASE_FIELDS
from tests.pytest_shared_support import STRUCTURED_EVENT_LIFECYCLES

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.integration.pytest_integration_support import StdinText
    from tests.pytest_shared_support import CliInvoke
    from tests.pytest_shared_support import JsonOutputParser

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]


# SECTION: HELPERS ========================================================== #


def _assert_base_event_fields(
    line: dict[str, Any],
    *,
    command: str,
    lifecycle: str,
) -> None:
    """Assert one emitted event keeps the stable base envelope."""
    assert STRUCTURED_EVENT_BASE_FIELDS.issubset(line)
    assert lifecycle in STRUCTURED_EVENT_LIFECYCLES
    assert line['command'] == command
    assert line['event'] == f'{command}.{lifecycle}'
    assert line['lifecycle'] == lifecycle
    assert line['schema'] == EVENT_SCHEMA
    assert line['schema_version'] == EVENT_SCHEMA_VERSION
    assert isinstance(line['run_id'], str)
    assert datetime.fromisoformat(line['timestamp'])


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

    def test_data_operation_failure_without_event_format_keeps_stderr_human_only(
        self,
        cli_invoke: CliInvoke,
    ) -> None:
        """Structured events should stay disabled unless explicitly requested."""
        code, out, err = cli_invoke(('extract', 'missing-contract.json'))

        assert code == 1
        assert out == ''
        assert parse_jsonl_event_lines(err) == []
        assert 'Error: File not found: missing-contract.json' in err

    @pytest.mark.parametrize(
        (
            'command',
            'args',
            'stdin_payload',
            'command_fields',
            'error_type',
            'error_message_contains',
        ),
        [
            pytest.param(
                'extract',
                ('extract', '--event-format', 'jsonl', 'missing-contract.json'),
                None,
                {
                    'source': 'missing-contract.json',
                    'source_type': 'file',
                },
                'FileNotFoundError',
                'File not found',
                id='extract-missing-file',
            ),
            pytest.param(
                'load',
                (
                    'load',
                    '--event-format',
                    'jsonl',
                    '--target-type',
                    'file',
                    'output.unknownext',
                ),
                '{"id": 1}',
                {
                    'source': '-',
                    'target': 'output.unknownext',
                    'target_type': 'file',
                },
                'ValueError',
                "Cannot infer file format from extension '.unknownext'",
                id='load-unknown-target-format',
            ),
            pytest.param(
                'transform',
                (
                    'transform',
                    '--event-format',
                    'jsonl',
                    '--operations',
                    '[]',
                    '[{"id": 1}]',
                    '-',
                ),
                None,
                {
                    'source': '[{"id": 1}]',
                    'target': 'stdout',
                    'target_type': 'file',
                },
                'ValueError',
                'operations must resolve to a mapping of transforms',
                id='transform-invalid-operations-shape',
            ),
            pytest.param(
                'validate',
                (
                    'validate',
                    '--event-format',
                    'jsonl',
                    '--rules',
                    '[]',
                    '[{"id": 1}]',
                ),
                None,
                {
                    'source': '[{"id": 1}]',
                    'target': 'stdout',
                },
                'ValueError',
                'rules must resolve to a mapping of field rules',
                id='validate-invalid-rules-shape',
            ),
        ],
    )
    def test_data_operation_failures_keep_event_and_error_stream_contract(
        self,
        cli_invoke: CliInvoke,
        stdin_text: StdinText,
        command: str,
        args: tuple[str, ...],
        stdin_payload: str | None,
        command_fields: dict[str, object],
        error_type: str,
        error_message_contains: str,
    ) -> None:
        """Data-op failures should emit events plus human errors on STDERR."""
        if stdin_payload is not None:
            stdin_text(stdin_payload)

        code, out, err = cli_invoke(args)

        assert code == 1
        assert out == ''
        lines = parse_jsonl_event_lines(err)
        _assert_failed_lifecycle(
            lines,
            command=command,
            command_fields=command_fields,
            error_type=error_type,
            error_message_contains=error_message_contains,
        )
        assert f'Error: {error_message_contains}' in err

    @pytest.mark.parametrize(
        'command',
        [
            pytest.param('extract', id='extract'),
            pytest.param('load', id='load'),
            pytest.param('transform', id='transform'),
            pytest.param('validate', id='validate'),
        ],
    )
    def test_data_operations_emit_jsonl_events(
        self,
        cli_invoke: CliInvoke,
        json_payload_file: Path,
        operations_json: str,
        parse_json_output: JsonOutputParser,
        rules_json: str,
        sample_records: list[dict[str, Any]],
        sample_records_json: str,
        stdin_text: StdinText,
        tmp_path: Path,
        command: str,
    ) -> None:
        """Test that data operations emit stable ``etlplus.event.v1`` JSONL."""
        out_path = tmp_path / 'load-events.json'
        operation_cases = {
            'extract': (
                ('extract', '--event-format', 'jsonl', str(json_payload_file)),
                None,
                sample_records,
                {'source': str(json_payload_file), 'source_type': 'file'},
            ),
            'load': (
                (
                    'load',
                    '--event-format',
                    'jsonl',
                    '--target-type',
                    'file',
                    str(out_path),
                ),
                sample_records_json,
                {'status': 'success'},
                {'source': '-', 'target': str(out_path), 'target_type': 'file'},
            ),
            'transform': (
                (
                    'transform',
                    '--event-format',
                    'jsonl',
                    '--operations',
                    operations_json,
                    '-',
                    '-',
                ),
                sample_records_json,
                list,
                {'source': '-', 'target': 'stdout', 'target_type': 'file'},
            ),
            'validate': (
                (
                    'validate',
                    '--event-format',
                    'jsonl',
                    '--rules',
                    rules_json,
                    '-',
                ),
                sample_records_json,
                {'valid': True},
                {'source': '-', 'target': 'stdout'},
            ),
        }
        args, stdin_payload, expected_output, command_fields = operation_cases[command]
        if stdin_payload is not None:
            stdin_text(stdin_payload)

        code, out, err = cli_invoke(args)

        assert code == 0
        payload = parse_json_output(out)
        if isinstance(expected_output, type):
            assert isinstance(payload, expected_output)
        elif isinstance(expected_output, dict):
            assert {
                key: payload[key] for key in expected_output
            } == expected_output
        else:
            assert payload == expected_output
        _assert_success_lifecycle(
            parse_jsonl_event_lines(err),
            command=command,
            command_fields=command_fields,
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
        lines = parse_jsonl_event_lines(err)

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
        lines = parse_jsonl_event_lines(err)

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
        lines = parse_jsonl_event_lines(err)

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
