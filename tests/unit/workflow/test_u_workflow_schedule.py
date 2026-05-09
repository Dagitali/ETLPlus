"""
:mod:`tests.unit.workflow.test_u_workflow_schedule` module.

Unit tests for :mod:`etlplus.workflow._schedule`.
"""

from __future__ import annotations

import pytest

from etlplus.workflow._schedule import ScheduleBackfillConfig
from etlplus.workflow._schedule import ScheduleConfig
from etlplus.workflow._schedule import ScheduleIntervalConfig
from etlplus.workflow._schedule import ScheduleTargetConfig
from etlplus.workflow._schedule import schedule_validation_issues

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


# SECTION: TESTS ============================================================ #


class TestScheduleParsing:
    """Unit tests for workflow schedule dataclass parsing."""

    @pytest.mark.parametrize(
        ('schedule_cls', 'payload', 'expected'),
        [
            pytest.param(
                ScheduleIntervalConfig,
                {'minutes': '15'},
                ScheduleIntervalConfig(minutes=15),
                id='interval-coerces-minutes',
            ),
            pytest.param(
                ScheduleIntervalConfig,
                {'minutes': 0},
                ScheduleIntervalConfig(minutes=1),
                id='interval-clamps-to-minimum',
            ),
            pytest.param(
                ScheduleTargetConfig,
                {'job': 'daily-sync', 'run_all': 'false'},
                ScheduleTargetConfig(job='daily-sync', run_all=False),
                id='target-coerces-run-all-flag',
            ),
            pytest.param(
                ScheduleBackfillConfig,
                {'enabled': 'yes', 'max_catchup_runs': '3', 'start_at': 20260508},
                ScheduleBackfillConfig(
                    enabled=True,
                    max_catchup_runs=3,
                    start_at='20260508',
                ),
                id='backfill-coerces-fields',
            ),
            pytest.param(
                ScheduleConfig,
                {
                    'name': 'weekday-refresh',
                    'cron': '0 6 * * *',
                    'timezone': 'UTC',
                    'paused': 'on',
                    'target': {'job': 'refresh-warehouse'},
                    'backfill': {'enabled': True, 'max_catchup_runs': 2},
                },
                ScheduleConfig(
                    name='weekday-refresh',
                    cron='0 6 * * *',
                    timezone='UTC',
                    paused=True,
                    target=ScheduleTargetConfig(job='refresh-warehouse', run_all=False),
                    backfill=ScheduleBackfillConfig(
                        enabled=True,
                        max_catchup_runs=2,
                        start_at=None,
                    ),
                ),
                id='schedule-parses-nested-config',
            ),
        ],
    )
    def test_from_obj_valid(
        self,
        schedule_cls: type[
            ScheduleIntervalConfig
            | ScheduleTargetConfig
            | ScheduleBackfillConfig
            | ScheduleConfig
        ],
        payload: dict[str, object],
        expected: object,
    ) -> None:
        """Test valid schedule payloads parse into the expected dataclasses."""
        assert schedule_cls.from_obj(payload) == expected

    @pytest.mark.parametrize(
        ('schedule_cls', 'payload'),
        [
            pytest.param(ScheduleIntervalConfig, None, id='interval-none'),
            pytest.param(
                ScheduleIntervalConfig,
                {'minutes': 'invalid'},
                id='interval-invalid-minutes',
            ),
            pytest.param(ScheduleTargetConfig, None, id='target-none'),
            pytest.param(ScheduleBackfillConfig, None, id='backfill-none'),
            pytest.param(ScheduleConfig, None, id='schedule-none'),
            pytest.param(
                ScheduleConfig,
                {'cron': '0 0 * * *'},
                id='schedule-missing-name',
            ),
        ],
    )
    def test_from_obj_invalid(
        self,
        schedule_cls: type[
            ScheduleIntervalConfig
            | ScheduleTargetConfig
            | ScheduleBackfillConfig
            | ScheduleConfig
        ],
        payload: dict[str, object] | None,
    ) -> None:
        """Test invalid schedule payloads yield ``None`` where expected."""
        assert schedule_cls.from_obj(payload) is None


class TestScheduleValidation:
    """Unit tests for schedule semantic validation."""

    @pytest.mark.parametrize(
        ('schedules', 'job_names', 'expected_issues'),
        [
            pytest.param(
                [
                    ScheduleConfig(
                        name='nightly',
                        cron='0 0 * * *',
                        target=ScheduleTargetConfig(job='sync-db'),
                    ),
                ],
                {'sync-db'},
                [],
                id='valid-cron-job-target',
            ),
            pytest.param(
                [
                    ScheduleConfig(
                        name='nightly',
                        cron='0 0 * * *',
                        target=ScheduleTargetConfig(job='sync-db'),
                    ),
                    ScheduleConfig(
                        name='nightly',
                        interval=ScheduleIntervalConfig(minutes=10),
                        target=ScheduleTargetConfig(run_all=True),
                    ),
                ],
                {'sync-db'},
                ['duplicate schedule name: nightly'],
                id='duplicate-schedule-name',
            ),
            pytest.param(
                [
                    ScheduleConfig(
                        name='broken-trigger',
                        target=ScheduleTargetConfig(job='sync-db'),
                    ),
                ],
                {'sync-db'},
                ['schedule must define exactly one trigger: cron or interval'],
                id='missing-trigger',
            ),
            pytest.param(
                [
                    ScheduleConfig(
                        name='missing-target',
                        cron='0 0 * * *',
                    ),
                ],
                {'sync-db'},
                ['schedule must define a target'],
                id='missing-target',
            ),
            pytest.param(
                [
                    ScheduleConfig(
                        name='invalid-target-mode',
                        cron='0 0 * * *',
                        target=ScheduleTargetConfig(job='sync-db', run_all=True),
                    ),
                ],
                {'sync-db'},
                ['schedule target must define exactly one mode: job or run_all'],
                id='conflicting-target-modes',
            ),
            pytest.param(
                [
                    ScheduleConfig(
                        name='unknown-job',
                        cron='0 0 * * *',
                        target=ScheduleTargetConfig(job='missing-job'),
                    ),
                ],
                {'sync-db'},
                ['unknown scheduled job reference: missing-job'],
                id='unknown-job-reference',
            ),
            pytest.param(
                [
                    ScheduleConfig(
                        name='bad-cron-length',
                        cron='0 0 * *',
                        target=ScheduleTargetConfig(run_all=True),
                    ),
                ],
                {'sync-db'},
                ['cron helper emission currently supports exactly five cron fields'],
                id='unsupported-cron-field-count',
            ),
            pytest.param(
                [
                    ScheduleConfig(
                        name='bad-cron-token',
                        cron='*/5 0 * * *',
                        target=ScheduleTargetConfig(run_all=True),
                    ),
                ],
                {'sync-db'},
                [
                    'cron helper emission currently supports only single '
                    'values or "*" fields',
                ],
                id='unsupported-cron-token',
            ),
        ],
    )
    def test_schedule_validation_issues(
        self,
        schedules: list[ScheduleConfig],
        job_names: set[str],
        expected_issues: list[str],
    ) -> None:
        """Test schedule validation emits the expected issue messages."""
        assert [
            issue['issue']
            for issue in schedule_validation_issues(schedules, job_names=job_names)
        ] == expected_issues
