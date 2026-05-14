"""
:mod:`tests.unit.runtime.test_u_runtime_scheduler` module.

Unit tests for :mod:`etlplus.runtime._scheduler`.
"""

from __future__ import annotations

import json
import os
from datetime import UTC
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from etlplus import Config
from etlplus.runtime import _scheduler as scheduler_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestDueRequests:
    """Unit tests for due schedule request selection."""

    def test_cron_field_matches_rejects_non_numeric_literals(self) -> None:
        """Malformed numeric cron fields should fail closed instead of raising."""
        assert scheduler_mod._cron_field_matches('not-a-number', 15) is False

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

        assert not skipped
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


class TestSchedulerLock:
    """Unit tests for the local scheduler file lock."""

    def test_acquire_reclaims_stale_lock_for_dead_pid(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Dead-process lock files should be treated as stale and reclaimed."""
        lock = scheduler_mod._ScheduleLock(tmp_path, 'nightly-all')
        lock._lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock._lock_path.write_text(
            json.dumps(
                {
                    'created_at': '2026-05-12T00:00:00+00:00',
                    'pid': 424242,
                },
            ),
            encoding='utf-8',
        )
        monkeypatch.setattr(
            scheduler_mod.os,
            'kill',
            lambda pid, _sig: (
                (_ for _ in ()).throw(ProcessLookupError()) if pid == 424242 else None
            ),
        )

        assert lock.acquire() is True

        persisted = json.loads(lock._lock_path.read_text(encoding='utf-8'))
        assert persisted['pid'] == os.getpid()
        assert isinstance(persisted['created_at'], str)
        lock.release()

    def test_acquire_keeps_malformed_lock_payload_authoritative(
        self,
        tmp_path: Path,
    ) -> None:
        """Malformed lock payloads should not be reclaimed speculatively."""
        lock = scheduler_mod._ScheduleLock(tmp_path, 'nightly-all')
        lock._lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock._lock_path.write_text('{not-json', encoding='utf-8')

        assert lock.acquire() is False
        assert lock._lock_path.read_text(encoding='utf-8') == '{not-json'

    def test_acquire_keeps_lock_when_pid_check_is_permission_denied(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Permission failures should keep the existing lock authoritative."""
        lock = scheduler_mod._ScheduleLock(tmp_path, 'nightly-all')
        lock._lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock._lock_path.write_text(
            json.dumps(
                {
                    'created_at': '2026-05-12T00:00:00+00:00',
                    'pid': 424242,
                },
            ),
            encoding='utf-8',
        )
        monkeypatch.setattr(
            scheduler_mod.os,
            'kill',
            lambda pid, _sig: (
                (_ for _ in ()).throw(PermissionError()) if pid == 424242 else None
            ),
        )

        assert lock.acquire() is False

    @pytest.mark.parametrize(
        ('schedule_name', 'expected_requests', 'expected_skipped'),
        [
            pytest.param(
                None,
                [
                    scheduler_mod.ScheduledRunRequest(
                        catchup=False,
                        job_name='seed',
                        run_all=False,
                        schedule_name='seed-every-15m',
                        trigger='interval',
                        triggered_at='2026-05-12T00:15:00+00:00',
                    ),
                ],
                [
                    {
                        'catchup': False,
                        'job': 'seed',
                        'reason': 'paused',
                        'schedule': 'paused-seed',
                        'status': 'skipped',
                        'trigger': 'interval',
                        'triggered_at': '2026-05-12T00:15:00+00:00',
                    },
                ],
                id='returns-due-and-paused-schedules',
            ),
            pytest.param(
                'seed-every-15m',
                [
                    scheduler_mod.ScheduledRunRequest(
                        catchup=False,
                        job_name='seed',
                        run_all=False,
                        schedule_name='seed-every-15m',
                        trigger='interval',
                        triggered_at='2026-05-12T00:15:00+00:00',
                    ),
                ],
                [],
                id='filters-to-selected-schedule',
            ),
        ],
    )
    def test_due_requests_normalizes_schedule_shape_once(
        self,
        tmp_path: Path,
        schedule_name: str | None,
        expected_requests: list[scheduler_mod.ScheduledRunRequest],
        expected_skipped: list[dict[str, object]],
    ) -> None:
        """Valid schedules should share one normalized request-building path."""
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
                    },
                    {
                        'name': 'paused-seed',
                        'interval': {'minutes': 15},
                        'paused': True,
                        'target': {'job': 'seed'},
                    },
                    {
                        'name': 'ignored-missing-target',
                        'interval': {'minutes': 15},
                    },
                ],
            },
        )
        state_store = scheduler_mod._SchedulerStateStore(tmp_path)
        state_store.record_trigger(
            schedule_name='seed-every-15m',
            triggered_at='2026-05-12T00:00:00+00:00',
        )

        requests, skipped = scheduler_mod.LocalScheduler.due_requests(
            cfg,
            now=datetime(2026, 5, 12, 0, 15, tzinfo=UTC),
            schedule_name=schedule_name,
            state_store=state_store,
        )

        assert requests == expected_requests
        assert skipped == expected_skipped

    def test_release_without_acquire_is_a_safe_noop(self, tmp_path: Path) -> None:
        """Releasing an unacquired lock should still clean up without raising."""
        lock = scheduler_mod._ScheduleLock(tmp_path, 'nightly-all')

        lock.release()

        assert not (
            tmp_path / scheduler_mod._SCHEDULER_LOCK_DIR / 'nightly-all.lock'
        ).exists()


class TestRunPending:
    """Unit tests for local schedule-trigger execution."""

    def test_run_pending_consumes_trigger_after_handled_callback_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Nonzero callback exits should still consume the dispatched trigger."""
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
                            'max_catchup_runs': 1,
                            'start_at': '2026-05-12T00:00:00+00:00',
                        },
                    },
                ],
            },
        )

        monkeypatch.setattr(
            scheduler_mod.LocalScheduler,
            'utc_now',
            staticmethod(lambda: datetime(2026, 5, 12, 0, 1, tzinfo=UTC)),
        )

        payload = scheduler_mod.LocalScheduler.run_pending(
            cfg=cfg,
            config_path='pipeline.yml',
            event_format=None,
            pretty=False,
            run_callback=lambda **_kwargs: 2,
            state_dir=tmp_path,
        )

        assert payload['runs'] == [
            {
                'catchup': True,
                'job': 'seed',
                'schedule': 'seed-every-15m',
                'status': 'error',
                'trigger': 'interval',
                'triggered_at': '2026-05-12T00:00:00+00:00',
            },
        ]
        state_payload = json.loads((tmp_path / 'scheduler-state.json').read_text())
        assert state_payload == {
            'schedules': {
                'seed-every-15m': {
                    'last_attempted_at': '2026-05-12T00:00:00+00:00',
                    'last_completed_at': '2026-05-12T00:00:00+00:00',
                    'last_status': 'error',
                },
            },
        }
        requests, skipped = scheduler_mod.LocalScheduler.due_requests(
            cfg,
            now=datetime(2026, 5, 12, 0, 1, tzinfo=UTC),
            schedule_name=None,
            state_store=scheduler_mod._SchedulerStateStore(tmp_path),
        )
        assert skipped == []
        assert requests == []

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
            recorder({'run_id': f'run-{len(dispatch_calls)}', 'status': 'ok'})
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
                    'last_attempted_at': '2026-05-12T00:15:00+00:00',
                    'last_completed_at': '2026-05-12T00:15:00+00:00',
                    'last_run_id': 'run-2',
                    'last_status': 'ok',
                },
            },
        }

    def test_run_pending_does_not_consume_trigger_when_callback_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Callback exceptions should leave the due trigger eligible for replay."""
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
                            'max_catchup_runs': 1,
                            'start_at': '2026-05-12T00:00:00+00:00',
                        },
                    },
                ],
            },
        )

        monkeypatch.setattr(
            scheduler_mod.LocalScheduler,
            'utc_now',
            staticmethod(lambda: datetime(2026, 5, 12, 0, 1, tzinfo=UTC)),
        )

        with pytest.raises(RuntimeError, match='dispatch failed'):
            scheduler_mod.LocalScheduler.run_pending(
                cfg=cfg,
                config_path='pipeline.yml',
                event_format=None,
                pretty=False,
                run_callback=lambda **_kwargs: (_ for _ in ()).throw(
                    RuntimeError('dispatch failed'),
                ),
                state_dir=tmp_path,
            )

        state_payload = json.loads((tmp_path / 'scheduler-state.json').read_text())
        assert state_payload == {
            'schedules': {
                'seed-every-15m': {
                    'last_error_message': 'dispatch failed',
                    'last_error_type': 'RuntimeError',
                    'last_attempted_at': '2026-05-12T00:00:00+00:00',
                    'last_status': 'exception',
                },
            },
        }
        requests, skipped = scheduler_mod.LocalScheduler.due_requests(
            cfg,
            now=datetime(2026, 5, 12, 0, 1, tzinfo=UTC),
            schedule_name=None,
            state_store=scheduler_mod._SchedulerStateStore(tmp_path),
        )
        assert skipped == []
        assert requests == [
            scheduler_mod.ScheduledRunRequest(
                catchup=True,
                job_name='seed',
                run_all=False,
                schedule_name='seed-every-15m',
                trigger='interval',
                triggered_at='2026-05-12T00:00:00+00:00',
            ),
        ]

    def test_run_pending_raises_partial_summary_for_catchup_exception(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Catch-up exceptions should return a partial summary via the raised error."""
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
                            'max_catchup_runs': 3,
                            'start_at': '2026-05-12T00:00:00+00:00',
                        },
                    },
                ],
            },
        )
        calls = 0

        monkeypatch.setattr(
            scheduler_mod.LocalScheduler,
            'utc_now',
            staticmethod(lambda: datetime(2026, 5, 12, 0, 31, tzinfo=UTC)),
        )

        def _run_callback(**kwargs: object) -> int:
            nonlocal calls
            calls += 1
            if calls == 1:
                recorder = kwargs.get('result_recorder')
                assert callable(recorder)
                recorder({'run_id': 'run-1', 'status': 'ok'})
                return 0
            raise RuntimeError('dispatch failed')

        with pytest.raises(scheduler_mod.SchedulerDispatchError) as exc_info:
            scheduler_mod.LocalScheduler.run_pending(
                cfg=cfg,
                config_path='pipeline.yml',
                event_format=None,
                pretty=False,
                run_callback=_run_callback,
                state_dir=tmp_path,
            )

        payload = exc_info.value.payload
        assert payload['due_count'] == 3
        assert payload['attempted_count'] == 2
        assert payload['completed_count'] == 1
        assert payload['pending_count'] == 2
        assert payload['stopped_early'] is True
        assert payload['runs'] == [
            {
                'catchup': True,
                'job': 'seed',
                'run_id': 'run-1',
                'schedule': 'seed-every-15m',
                'status': 'ok',
                'trigger': 'interval',
                'triggered_at': '2026-05-12T00:00:00+00:00',
            },
            {
                'catchup': True,
                'error_message': 'dispatch failed',
                'error_type': 'RuntimeError',
                'job': 'seed',
                'reason': 'exception',
                'schedule': 'seed-every-15m',
                'status': 'error',
                'trigger': 'interval',
                'triggered_at': '2026-05-12T00:15:00+00:00',
            },
        ]
        assert payload['pending_runs'] == [
            {
                'catchup': True,
                'error_message': 'dispatch failed',
                'error_type': 'RuntimeError',
                'job': 'seed',
                'reason': 'exception',
                'schedule': 'seed-every-15m',
                'status': 'pending',
                'trigger': 'interval',
                'triggered_at': '2026-05-12T00:15:00+00:00',
            },
            {
                'catchup': True,
                'job': 'seed',
                'reason': 'deferred',
                'schedule': 'seed-every-15m',
                'status': 'pending',
                'trigger': 'interval',
                'triggered_at': '2026-05-12T00:30:00+00:00',
            },
        ]

    def test_run_pending_success_clears_exception_metadata_after_recovery(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """A successful retry should clear the stale exception diagnostics."""
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
                            'max_catchup_runs': 1,
                            'start_at': '2026-05-12T00:00:00+00:00',
                        },
                    },
                ],
            },
        )

        monkeypatch.setattr(
            scheduler_mod.LocalScheduler,
            'utc_now',
            staticmethod(lambda: datetime(2026, 5, 12, 0, 1, tzinfo=UTC)),
        )

        with pytest.raises(scheduler_mod.SchedulerDispatchError):
            scheduler_mod.LocalScheduler.run_pending(
                cfg=cfg,
                config_path='pipeline.yml',
                event_format=None,
                pretty=False,
                run_callback=lambda **_kwargs: (_ for _ in ()).throw(
                    RuntimeError('dispatch failed'),
                ),
                state_dir=tmp_path,
            )

        payload = scheduler_mod.LocalScheduler.run_pending(
            cfg=cfg,
            config_path='pipeline.yml',
            event_format=None,
            pretty=False,
            run_callback=lambda **kwargs: (
                kwargs['result_recorder']({'run_id': 'run-1', 'status': 'ok'}) or 0
            ),
            state_dir=tmp_path,
        )

        assert payload['completed_count'] == 1
        state_payload = json.loads((tmp_path / 'scheduler-state.json').read_text())
        assert state_payload == {
            'schedules': {
                'seed-every-15m': {
                    'last_attempted_at': '2026-05-12T00:00:00+00:00',
                    'last_completed_at': '2026-05-12T00:00:00+00:00',
                    'last_run_id': 'run-1',
                    'last_status': 'ok',
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
        (lock_dir / 'nightly-all.lock').write_text(
            json.dumps(
                {
                    'created_at': '2026-05-12T00:00:00+00:00',
                    'pid': os.getpid(),
                },
            ),
            encoding='utf-8',
        )

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


class TestSchedulerInternals:
    """Unit tests for internal scheduler helper branches."""

    @pytest.mark.parametrize(
        ('schedule', 'expected'),
        [
            pytest.param(SimpleNamespace(backfill=None), 1, id='missing-backfill'),
            pytest.param(
                SimpleNamespace(
                    backfill=SimpleNamespace(enabled=False, max_catchup_runs=4),
                ),
                1,
                id='disabled-backfill',
            ),
            pytest.param(
                SimpleNamespace(
                    backfill=SimpleNamespace(enabled=True, max_catchup_runs=0),
                ),
                1,
                id='invalid-catchup-limit-falls-back-to-one',
            ),
            pytest.param(
                SimpleNamespace(
                    backfill=SimpleNamespace(enabled=True, max_catchup_runs=3),
                ),
                3,
                id='positive-catchup-limit',
            ),
        ],
    )
    def test_backfill_limit_normalizes_disabled_and_invalid_values(
        self,
        schedule: object,
        expected: int,
    ) -> None:
        """Backfill limit resolution should clamp missing and invalid values."""
        assert scheduler_mod._backfill_limit(schedule) == expected

    def test_cron_due_times_fall_back_to_current_minute_for_invalid_backfill_start(
        self,
    ) -> None:
        """Invalid cron backfill start values should fall back to the current minute."""
        schedule = SimpleNamespace(
            cron='0 * * * *',
            timezone='UTC',
            backfill=SimpleNamespace(
                enabled=True,
                max_catchup_runs=2,
                start_at='not-a-timestamp',
            ),
        )

        assert scheduler_mod.LocalScheduler._cron_due_times(
            schedule,
            now=datetime(2026, 5, 12, 2, 0, tzinfo=UTC),
            previous_triggered_at=None,
        ) == [datetime(2026, 5, 12, 2, 0, tzinfo=UTC)]

    def test_cron_due_times_uses_backfill_start_and_previous_trigger_bounds(
        self,
    ) -> None:
        """Cron due times should respect both configured backfill and prior triggers."""
        schedule = SimpleNamespace(
            cron='0 * * * *',
            timezone='UTC',
            backfill=SimpleNamespace(
                enabled=True,
                max_catchup_runs=2,
                start_at='2026-05-12T00:00:00+00:00',
            ),
        )

        assert scheduler_mod.LocalScheduler._cron_due_times(
            schedule,
            now=datetime(2026, 5, 12, 2, 0, tzinfo=UTC),
            previous_triggered_at=None,
        ) == [
            datetime(2026, 5, 12, 1, 0, tzinfo=UTC),
            datetime(2026, 5, 12, 2, 0, tzinfo=UTC),
        ]
        assert scheduler_mod.LocalScheduler._cron_due_times(
            schedule,
            now=datetime(2026, 5, 12, 2, 0, tzinfo=UTC),
            previous_triggered_at='2026-05-12T01:00:00+00:00',
        ) == [datetime(2026, 5, 12, 2, 0, tzinfo=UTC)]

    @pytest.mark.parametrize(
        ('field', 'value', 'expected'),
        [
            pytest.param('*', 5, True, id='wildcard'),
            pytest.param('5', 5, True, id='exact-match'),
            pytest.param('7', 5, False, id='exact-mismatch'),
            pytest.param('bad', 5, False, id='invalid-field'),
        ],
    )
    def test_cron_field_matches_handles_wildcards_and_invalid_fields(
        self,
        field: str,
        value: int,
        expected: bool,
    ) -> None:
        """Cron field matching should short-circuit wildcards and bad values."""

    @pytest.mark.parametrize(
        ('cron', 'when_local', 'expected'),
        [
            pytest.param(
                None,
                datetime(2026, 5, 11, 2, 0, tzinfo=UTC),
                False,
                id='missing-cron',
            ),
            pytest.param(
                '',
                datetime(2026, 5, 11, 2, 0, tzinfo=UTC),
                False,
                id='blank-cron',
            ),
            pytest.param(
                '0 2 * *',
                datetime(2026, 5, 11, 2, 0, tzinfo=UTC),
                False,
                id='wrong-field-count',
            ),
            pytest.param(
                '0 2 * * 7',
                datetime(2026, 5, 10, 2, 0, tzinfo=UTC),
                True,
                id='sunday-seven-alias',
            ),
        ],
    )
    def test_cron_matches_handles_invalid_and_sunday_alias_shapes(
        self,
        cron: str | None,
        when_local: datetime,
        expected: bool,
    ) -> None:
        """Cron matching should reject malformed crons and allow Sunday alias 7."""
        schedule = SimpleNamespace(cron=cron)

        assert scheduler_mod._cron_matches(schedule, when_local) is expected

    @pytest.mark.parametrize(
        ('schedule', 'previous_triggered_at', 'expected'),
        [
            pytest.param(
                SimpleNamespace(interval=SimpleNamespace(minutes=0), timezone='UTC'),
                None,
                [],
                id='invalid-interval-minutes',
            ),
            pytest.param(
                SimpleNamespace(
                    interval=SimpleNamespace(minutes=15),
                    timezone='UTC',
                    backfill=SimpleNamespace(
                        enabled=True,
                        max_catchup_runs=2,
                        start_at='2026-05-12T00:00:00+00:00',
                    ),
                ),
                None,
                [
                    datetime(2026, 5, 12, 0, 0, tzinfo=UTC),
                    datetime(2026, 5, 12, 0, 15, tzinfo=UTC),
                ],
                id='backfill-start-at-bounds-catchup-runs',
            ),
            pytest.param(
                SimpleNamespace(
                    interval=SimpleNamespace(minutes=15),
                    timezone='UTC',
                    backfill=SimpleNamespace(
                        enabled=True,
                        start_at='not-a-timestamp',
                    ),
                ),
                None,
                [datetime(2026, 5, 12, 0, 30, tzinfo=UTC)],
                id='invalid-backfill-start-falls-back-to-current-minute',
            ),
            pytest.param(
                SimpleNamespace(interval=SimpleNamespace(minutes=15), timezone='UTC'),
                '2026-05-12T00:30:00+00:00',
                [],
                id='previous-trigger-can-produce-no-due-times',
            ),
            pytest.param(
                SimpleNamespace(interval=SimpleNamespace(minutes=15), timezone='UTC'),
                None,
                [datetime(2026, 5, 12, 0, 30, tzinfo=UTC)],
                id='no-previous-or-backfill-runs-current-minute',
            ),
        ],
    )
    def test_interval_due_times_cover_invalid_backfill_and_previous_trigger_paths(
        self,
        schedule: object,
        previous_triggered_at: str | None,
        expected: list[datetime],
    ) -> None:
        """Interval due-time selection should cover its short-circuit branches."""
        assert (
            scheduler_mod.LocalScheduler._interval_due_times(
                schedule,
                now=datetime(2026, 5, 12, 0, 30, tzinfo=UTC),
                previous_triggered_at=previous_triggered_at,
            )
            == expected
        )

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param(None, None, id='missing-timestamp'),
            pytest.param('', None, id='blank-timestamp'),
            pytest.param('not-a-timestamp', None, id='invalid-timestamp'),
            pytest.param(
                '2026-05-12T00:15:00',
                datetime(2026, 5, 12, 0, 15, tzinfo=UTC),
                id='naive-timestamp-defaults-to-utc',
            ),
        ],
    )
    def test_parse_timestamp_handles_missing_invalid_and_naive_values(
        self,
        value: str | None,
        expected: datetime | None,
    ) -> None:
        """Timestamp parsing should reject invalid input and normalize naive values."""
        assert scheduler_mod._parse_timestamp(value) == expected

    @pytest.mark.parametrize(
        ('schedule', 'expected_name'),
        [
            pytest.param(
                SimpleNamespace(name=None, target=SimpleNamespace()),
                None,
                id='missing-name',
            ),
            pytest.param(
                SimpleNamespace(name='nightly', target=None),
                None,
                id='missing-target',
            ),
            pytest.param(
                SimpleNamespace(
                    name='nightly',
                    paused=True,
                    interval=SimpleNamespace(minutes=15),
                    target=SimpleNamespace(job=123, run_all=True),
                ),
                'nightly',
                id='coerces-non-string-job-to-none',
            ),
        ],
    )
    def test_resolve_schedule_validates_required_fields(
        self,
        schedule: object,
        expected_name: str | None,
    ) -> None:
        """Schedule normalization should reject incomplete shapes and normalize jobs."""
        resolved = scheduler_mod._resolve_schedule(schedule)

        if expected_name is None:
            assert resolved is None
            return

        assert resolved is not None
        assert resolved.name == expected_name
        assert resolved.job_name is None
        assert resolved.run_all is True
        assert resolved.paused is True
        assert resolved.trigger == 'interval'

    @pytest.mark.parametrize(
        ('timezone_name', 'expected_key'),
        [
            pytest.param(None, 'UTC', id='missing-timezone-defaults-to-utc'),
            pytest.param('UTC', 'UTC', id='valid-timezone'),
            pytest.param('Not/AZone', 'UTC', id='invalid-timezone-falls-back-to-utc'),
        ],
    )
    def test_resolve_timezone_defaults_and_falls_back_to_utc(
        self,
        timezone_name: str | None,
        expected_key: str,
    ) -> None:
        """Timezone resolution should fall back to UTC for missing or bad names."""
        assert (
            scheduler_mod._resolve_timezone(SimpleNamespace(timezone=timezone_name)).key
            == expected_key
        )

    def test_schedule_metadata_adds_optional_reason_and_run_id(self) -> None:
        """Scheduler metadata rows should include optional context only when present."""
        request = scheduler_mod.ScheduledRunRequest(
            catchup=True,
            job_name='seed',
            run_all=False,
            schedule_name='seed-every-15m',
            trigger='interval',
            triggered_at='2026-05-12T00:15:00+00:00',
        )

        assert scheduler_mod._schedule_metadata(
            request,
            status='error',
            reason='overlap',
            run_id='run-123',
        ) == {
            'catchup': True,
            'job': 'seed',
            'reason': 'overlap',
            'run_id': 'run-123',
            'schedule': 'seed-every-15m',
            'status': 'error',
            'trigger': 'interval',
            'triggered_at': '2026-05-12T00:15:00+00:00',
        }

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            pytest.param('[]', None, id='non-mapping-root'),
            pytest.param('{"schedules": []}', None, id='non-mapping-schedules'),
            pytest.param(
                '{"schedules": {"valid": {"last_triggered_at": "x"}, "bad": 1}}',
                'x',
                id='filters-invalid-schedule-state-rows',
            ),
        ],
    )
    def test_state_store_ignores_invalid_payload_shapes(
        self,
        tmp_path: Path,
        payload: str,
        expected: str | None,
    ) -> None:
        """Scheduler state loading should ignore malformed persisted payloads."""
        state_file = tmp_path / 'scheduler-state.json'
        state_file.write_text(payload, encoding='utf-8')

        assert (
            scheduler_mod._SchedulerStateStore(tmp_path).last_triggered_at('valid')
            == expected
        )

    def test_state_store_returns_current_state_payload(self, tmp_path: Path) -> None:
        """Scheduler state inspection should surface the stored metadata mapping."""
        state_store = scheduler_mod._SchedulerStateStore(tmp_path)
        state_store.record_attempt(
            schedule_name='nightly',
            triggered_at='2026-05-12T00:15:00+00:00',
        )
        state_store.record_completion(
            schedule_name='nightly',
            triggered_at='2026-05-12T00:15:00+00:00',
            status='ok',
            run_id='run-1',
        )

        assert state_store.state('nightly') == {
            'last_attempted_at': '2026-05-12T00:15:00+00:00',
            'last_completed_at': '2026-05-12T00:15:00+00:00',
            'last_run_id': 'run-1',
            'last_status': 'ok',
        }

    def test_state_store_treats_legacy_trigger_timestamp_as_completed_state(
        self,
        tmp_path: Path,
    ) -> None:
        """Legacy state files should remain readable as completed trigger state."""
        state_file = tmp_path / 'scheduler-state.json'
        state_file.write_text(
            '{"schedules": {"nightly": {"last_triggered_at": '
            '"2026-05-12T00:15:00+00:00"}}}',
            encoding='utf-8',
        )

        state_store = scheduler_mod._SchedulerStateStore(tmp_path)

        assert state_store.last_triggered_at('nightly') == '2026-05-12T00:15:00+00:00'

    def test_utc_now_returns_current_utc_datetime(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The UTC clock helper should delegate to ``datetime.now(UTC)``."""

        class _FakeDateTime:
            @staticmethod
            def now(tz: object) -> datetime:
                """Return one fixed UTC timestamp for the scheduler clock test."""
                assert tz is UTC
                return datetime(2026, 5, 12, 0, 45, tzinfo=UTC)

        monkeypatch.setattr(scheduler_mod, 'datetime', _FakeDateTime)

        assert scheduler_mod.LocalScheduler.utc_now() == datetime(
            2026,
            5,
            12,
            0,
            45,
            tzinfo=UTC,
        )
