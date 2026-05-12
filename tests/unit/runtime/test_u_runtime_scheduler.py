"""
:mod:`tests.unit.runtime.test_u_runtime_scheduler` module.

Unit tests for :mod:`etlplus.runtime._scheduler`.
"""

from __future__ import annotations

import json
from datetime import UTC
from datetime import datetime
from pathlib import Path

import pytest

from etlplus import Config
from etlplus.runtime import _scheduler as scheduler_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestDueRequests:
    """Unit tests for due schedule request selection."""

    def test_due_requests_returns_matching_cron_entry(
        self,
        tmp_path: Path,
    ) -> None:
        """Cron schedules due at the current minute should yield one request."""
        cfg = Config.from_dict(
            {
                'name': 'Scheduler Pipeline',
                'sources': [],
                'targets': [],
                'jobs': [],
                'schedules': [
                    {
                        'name': 'nightly-all',
                        'cron': '0 2 11 5 1',
                        'timezone': 'UTC',
                        'target': {'run_all': True},
                    },
                ],
            },
        )

        requests, skipped = scheduler_mod.LocalScheduler.due_requests(
            cfg,
            now=datetime(2026, 5, 11, 2, 0, tzinfo=UTC),
            schedule_name=None,
            state_store=scheduler_mod._SchedulerStateStore(tmp_path),
        )

        assert skipped == []
        assert requests == [
            scheduler_mod.ScheduledRunRequest(
                catchup=False,
                job_name=None,
                run_all=True,
                schedule_name='nightly-all',
                trigger='cron',
                triggered_at='2026-05-11T02:00:00+00:00',
            ),
        ]


class TestRunPending:
    """Unit tests for local schedule-trigger execution."""

    def test_run_pending_dispatches_due_interval_runs_and_persists_state(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Due interval schedules should dispatch bounded catch-up runs once."""
        cfg = Config.from_dict(
            {
                'name': 'Scheduler Pipeline',
                'sources': [],
                'targets': [],
                'jobs': [{'name': 'seed'}],
                'schedules': [
                    {
                        'name': 'seed-every-15m',
                        'interval': {'minutes': 15},
                        'target': {'job': 'seed'},
                        'backfill': {
                            'enabled': True,
                            'max_catchup_runs': 2,
                            'start_at': '2026-05-12T00:00:00+00:00',
                        },
                    },
                ],
            },
        )
        dispatch_calls: list[dict[str, object]] = []

        monkeypatch.setattr(
            scheduler_mod.LocalScheduler,
            'utc_now',
            staticmethod(lambda: datetime(2026, 5, 12, 0, 31, tzinfo=UTC)),
        )

        def _run_callback(**kwargs: object) -> int:
            dispatch_calls.append(dict(kwargs))
            recorder = kwargs.get('result_recorder')
            assert callable(recorder)
            recorder({'run_id': f"run-{len(dispatch_calls)}", 'status': 'ok'})
            return 0

        payload = scheduler_mod.LocalScheduler.run_pending(
            cfg=cfg,
            config_path='pipeline.yml',
            event_format='jsonl',
            pretty=False,
            run_callback=_run_callback,
            state_dir=tmp_path,
        )

        assert payload['dispatched_count'] == 2
        assert payload['skipped_count'] == 0
        assert [call['schedule_triggered_at'] for call in dispatch_calls] == [
            '2026-05-12T00:00:00+00:00',
            '2026-05-12T00:15:00+00:00',
        ]
        assert all(call['emit_output'] is False for call in dispatch_calls)
        assert all(call['schedule_name'] == 'seed-every-15m' for call in dispatch_calls)
        assert all(call['schedule_trigger'] == 'interval' for call in dispatch_calls)
        state_payload = json.loads((tmp_path / 'scheduler-state.json').read_text())
        assert state_payload == {
            'schedules': {
                'seed-every-15m': {
                    'last_triggered_at': '2026-05-12T00:15:00+00:00',
                },
            },
        }

    def test_run_pending_skips_locked_schedule(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Pre-existing schedule locks should prevent overlapping dispatch."""
        cfg = Config.from_dict(
            {
                'name': 'Scheduler Pipeline',
                'sources': [],
                'targets': [],
                'jobs': [],
                'schedules': [
                    {
                        'name': 'nightly-all',
                        'cron': '0 2 11 5 1',
                        'timezone': 'UTC',
                        'target': {'run_all': True},
                    },
                ],
            },
        )
        lock_dir = tmp_path / 'scheduler-locks'
        lock_dir.mkdir(parents=True)
        (lock_dir / 'nightly-all.lock').write_text('123\n', encoding='utf-8')

        monkeypatch.setattr(
            scheduler_mod.LocalScheduler,
            'utc_now',
            staticmethod(lambda: datetime(2026, 5, 11, 2, 0, tzinfo=UTC)),
        )

        payload = scheduler_mod.LocalScheduler.run_pending(
            cfg=cfg,
            config_path='pipeline.yml',
            event_format=None,
            pretty=False,
            run_callback=lambda **_kwargs: pytest.fail('dispatch should be skipped'),
            state_dir=tmp_path,
        )

        assert payload['dispatched_count'] == 0
        assert payload['skipped_count'] == 1
        assert payload['runs'] == [
            {
                'catchup': False,
                'run_all': True,
                'schedule': 'nightly-all',
                'status': 'skipped',
                'reason': 'overlap',
                'trigger': 'cron',
                'triggered_at': '2026-05-11T02:00:00+00:00',
            },
        ]
