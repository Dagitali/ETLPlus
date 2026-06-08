"""
:mod:`tests.integration.cli.test_i_cli_schedule` module.

Integration-scope regression tests for local scheduler runtime contracts.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC
from datetime import datetime
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest

from etlplus.cli._handlers import schedule as schedule_handler_mod
from tests.integration.cli.pytest_cli_integration_support import assert_cli_success
from tests.integration.cli.pytest_cli_integration_support import history_table_counts
from tests.integration.cli.pytest_cli_integration_support import parse_jsonl_event_lines

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.pytest_shared_support import CliInvoke
    from tests.pytest_shared_support import JsonOutputParser

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=protected-access

# SECTION: HELPERS ========================================================== #


def _write_scheduled_file_pipeline(
    tmp_path: Path,
    *,
    max_catchup_runs: int = 2,
    run_all: bool = False,
    schedule_name: str = 'seed-every-15m',
) -> Path:
    """Write one local file pipeline with an interval schedule."""
    input_path = tmp_path / 'input.json'
    output_path = tmp_path / 'output.json'
    input_path.write_text('[{"id": 1, "status": "new"}]', encoding='utf-8')
    target = 'run_all: true' if run_all else 'job: seed'
    config_path = tmp_path / 'scheduled-pipeline.yml'
    config_path.write_text(
        dedent(
            f"""
            name: Scheduled Runtime Regression
            sources:
              - name: src
                type: file
                format: json
                path: "{input_path}"
            targets:
              - name: dest
                type: file
                format: json
                path: "{output_path}"
            jobs:
              - name: seed
                extract:
                  source: src
                load:
                  target: dest
            schedules:
              - name: {schedule_name}
                interval:
                  minutes: 15
                target:
                  {target}
                backfill:
                  enabled: true
                  max_catchup_runs: {max_catchup_runs}
                  start_at: "2026-05-12T00:00:00+00:00"
            """,
        ).strip(),
        encoding='utf-8',
    )
    return config_path


# SECTION: TESTS ============================================================ #


class TestCliScheduleRunPending:
    """Integration regressions for ``etlplus schedule --run-pending``."""

    def test_run_pending_bounded_catchup_repeats_once_and_persists_metadata(
        self,
        cli_invoke: CliInvoke,
        monkeypatch: pytest.MonkeyPatch,
        parse_json_output: JsonOutputParser,
        tmp_path: Path,
    ) -> None:
        """
        Bounded catch-up should dispatch due triggers once and preserve
        scheduler metadata in events and local history.
        """
        state_dir = tmp_path / 'state'
        config_path = _write_scheduled_file_pipeline(tmp_path, run_all=True)
        monkeypatch.setenv('ETLPLUS_STATE_DIR', str(state_dir))
        monkeypatch.setattr(
            schedule_handler_mod.LocalScheduler,
            'utc_now',
            staticmethod(lambda: datetime(2026, 5, 12, 0, 16, tzinfo=UTC)),
        )

        first_code, first_out, first_err = cli_invoke(
            (
                'schedule',
                '--config',
                str(config_path),
                '--run-pending',
                '--event-format',
                'jsonl',
            ),
        )
        second_code, second_out, second_err = cli_invoke(
            (
                'schedule',
                '--config',
                str(config_path),
                '--run-pending',
                '--event-format',
                'jsonl',
            ),
        )

        assert first_code == 0
        assert_cli_success(second_code, second_err)
        first_payload = parse_json_output(first_out)
        second_payload = parse_json_output(second_out)
        assert first_payload['dispatched_count'] == 2
        assert [row['triggered_at'] for row in first_payload['runs']] == [
            '2026-05-12T00:00:00+00:00',
            '2026-05-12T00:15:00+00:00',
        ]
        assert [row['run_all'] for row in first_payload['runs']] == [True, True]
        assert second_payload['dispatched_count'] == 0
        assert second_payload['pending_runs'] == []

        events = parse_jsonl_event_lines(first_err)
        assert [event['event'] for event in events] == [
            'run.started',
            'run.completed',
            'run.started',
            'run.completed',
        ]
        assert {event['schedule'] for event in events} == {'seed-every-15m'}
        assert {event['schedule_trigger'] for event in events} == {'interval'}
        assert {event['schedule_catchup'] for event in events} == {True}
        assert [event['schedule_triggered_at'] for event in events[::2]] == [
            '2026-05-12T00:00:00+00:00',
            '2026-05-12T00:15:00+00:00',
        ]

        history_db = state_dir / 'history.sqlite'
        with sqlite3.connect(history_db) as conn:
            summaries = [
                json.loads(row[0])
                for row in conn.execute(
                    """
                    SELECT result_summary
                    FROM runs
                    ORDER BY started_at
                    """,
                ).fetchall()
            ]
            job_rows = conn.execute(
                """
                SELECT job_name, status
                FROM job_runs
                ORDER BY started_at
                """,
            ).fetchall()

        assert [summary['scheduler'] for summary in summaries] == [
            {
                'catchup': True,
                'schedule': 'seed-every-15m',
                'trigger': 'interval',
                'triggered_at': '2026-05-12T00:00:00+00:00',
            },
            {
                'catchup': True,
                'schedule': 'seed-every-15m',
                'trigger': 'interval',
                'triggered_at': '2026-05-12T00:15:00+00:00',
            },
        ]
        assert job_rows == [('seed', 'succeeded'), ('seed', 'succeeded')]
        assert history_table_counts(history_db) == (2, 2)

    def test_run_pending_overlap_remains_pending_without_history_writes(
        self,
        cli_invoke: CliInvoke,
        monkeypatch: pytest.MonkeyPatch,
        parse_json_output: JsonOutputParser,
        tmp_path: Path,
    ) -> None:
        """Overlap-skipped triggers should not create run-history rows."""
        state_dir = tmp_path / 'state'
        config_path = _write_scheduled_file_pipeline(
            tmp_path,
            max_catchup_runs=1,
            schedule_name='locked-seed',
        )
        lock_dir = state_dir / 'scheduler-locks'
        lock_dir.mkdir(parents=True)
        (lock_dir / 'locked-seed.lock').write_text(
            json.dumps({'created_at': '2026-05-12T00:00:00+00:00', 'pid': 1}),
            encoding='utf-8',
        )
        monkeypatch.setenv('ETLPLUS_STATE_DIR', str(state_dir))
        monkeypatch.setattr(
            schedule_handler_mod.LocalScheduler,
            'utc_now',
            staticmethod(lambda: datetime(2026, 5, 12, 0, 1, tzinfo=UTC)),
        )

        code, out, err = cli_invoke(
            ('schedule', '--config', str(config_path), '--run-pending'),
        )

        assert_cli_success(code, err)
        payload = parse_json_output(out)
        assert payload['completed_count'] == 0
        assert payload['pending_runs'] == [
            {
                'catchup': True,
                'job': 'seed',
                'reason': 'overlap',
                'schedule': 'locked-seed',
                'status': 'pending',
                'trigger': 'interval',
                'triggered_at': '2026-05-12T00:00:00+00:00',
            },
        ]
        assert not (state_dir / 'history.sqlite').exists()
