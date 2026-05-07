"""
:mod:`etlplus.workflow._schedule` module.

Data classes modeling portable schedule configuration.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any
from typing import Self

from ..utils import IntParser
from ..utils import MappingParser
from ..utils import ValueParser

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'ScheduleBackfillConfig',
    'ScheduleConfig',
    'ScheduleIntervalConfig',
    'ScheduleTargetConfig',
    # Functions
    'schedule_validation_issues',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_UNSUPPORTED_CRON_TOKENS = (',', '-', '/', '?')


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True, frozen=True)
class ScheduleIntervalConfig:
    """Interval-based schedule trigger."""

    # -- Instance Attributes -- #

    minutes: int

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: Any,
    ) -> Self | None:
        """Parse one interval mapping into a schedule interval config."""
        data = MappingParser.optional(obj)
        if not data:
            return None
        if (
            minutes := IntParser.parse(data.get('minutes'), default=None, minimum=1)
        ) is None:
            return None
        return cls(minutes=minutes)


@dataclass(kw_only=True, slots=True, frozen=True)
class ScheduleTargetConfig:
    """Execution target for one portable schedule entry."""

    # -- Instance Attributes -- #

    job: str | None = None
    run_all: bool = False

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: Any,
    ) -> Self | None:
        """Parse one schedule target mapping."""
        data = MappingParser.optional(obj)
        if not data:
            return None
        return cls(
            job=ValueParser.optional_str(data.get('job')),
            run_all=ValueParser.bool_flag(data.get('run_all'), default=False),
        )


@dataclass(kw_only=True, slots=True, frozen=True)
class ScheduleBackfillConfig:
    """Optional bounded backfill metadata for one schedule."""

    # -- Instance Attributes -- #

    enabled: bool = False
    max_catchup_runs: int | None = None
    start_at: str | None = None

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: Any,
    ) -> Self | None:
        """Parse one optional backfill mapping."""
        data = MappingParser.optional(obj)
        if not data:
            return None
        return cls(
            enabled=ValueParser.bool_flag(data.get('enabled'), default=False),
            max_catchup_runs=IntParser.parse(
                data.get('max_catchup_runs'),
                default=None,
                minimum=1,
            ),
            start_at=ValueParser.optional_str(data.get('start_at')),
        )


@dataclass(kw_only=True, slots=True, frozen=True)
class ScheduleConfig:
    """Portable schedule configuration attached to one ETLPlus config."""

    # -- Instance Attributes -- #

    name: str
    cron: str | None = None
    interval: ScheduleIntervalConfig | None = None
    timezone: str | None = None
    paused: bool = False
    target: ScheduleTargetConfig | None = None
    backfill: ScheduleBackfillConfig | None = None

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: Any,
    ) -> Self | None:
        """Parse one schedule mapping."""
        data = MappingParser.optional(obj)
        if not data:
            return None
        if (name := ValueParser.optional_str(data.get('name'))) is None:
            return None
        return cls(
            name=name,
            cron=ValueParser.optional_str(data.get('cron')),
            interval=ScheduleIntervalConfig.from_obj(data.get('interval')),
            timezone=ValueParser.optional_str(data.get('timezone')),
            paused=ValueParser.bool_flag(data.get('paused'), default=False),
            target=ScheduleTargetConfig.from_obj(data.get('target')),
            backfill=ScheduleBackfillConfig.from_obj(data.get('backfill')),
        )


# SECTION: FUNCTIONS ======================================================== #


def schedule_validation_issues(
    schedules: Iterable[ScheduleConfig],
    *,
    job_names: set[str],
) -> list[dict[str, str]]:
    """
    Return semantic validation issues for configured schedules.

    Parameters
    ----------
    schedules : Iterable[ScheduleConfig]
        Iterable of schedule configs to validate.
    job_names : set[str]
        Set of valid job names for schedule target reference checks.

    Returns
    -------
    list[dict[str, str]]
        List of validation issues with schedule name and issue description.
    """
    issues: list[dict[str, str]] = []
    seen_schedule_names: set[str] = set()

    for schedule in schedules:
        schedule_name = schedule.name
        if schedule_name in seen_schedule_names:
            issues.append(
                {
                    'issue': f'duplicate schedule name: {schedule_name}',
                    'schedule': schedule_name,
                },
            )
        else:
            seen_schedule_names.add(schedule_name)

        trigger_count = int(
            isinstance(schedule.cron, str) and bool(schedule.cron),
        ) + int(schedule.interval is not None)
        if trigger_count != 1:
            issues.append(
                {
                    'issue': (
                        'schedule must define exactly one trigger: cron or interval'
                    ),
                    'schedule': schedule_name,
                },
            )

        target = schedule.target
        if target is None:
            issues.append(
                {
                    'issue': 'schedule must define a target',
                    'schedule': schedule_name,
                },
            )
        else:
            target_count = int(bool(target.job)) + int(target.run_all)
            if target_count != 1:
                issues.append(
                    {
                        'issue': (
                            'schedule target must define exactly one mode: '
                            'job or run_all'
                        ),
                        'schedule': schedule_name,
                    },
                )
            if target.job and target.job not in job_names:
                issues.append(
                    {
                        'issue': f'unknown scheduled job reference: {target.job}',
                        'schedule': schedule_name,
                    },
                )

        cron = schedule.cron
        if not isinstance(cron, str) or not cron:
            continue
        cron_fields = cron.split()
        if len(cron_fields) != 5:
            issues.append(
                {
                    'issue': (
                        'cron helper emission currently supports exactly '
                        'five cron fields'
                    ),
                    'schedule': schedule_name,
                },
            )
            continue
        if any(
            any(token in field for token in _UNSUPPORTED_CRON_TOKENS)
            for field in cron_fields
        ):
            issues.append(
                {
                    'issue': (
                        'cron helper emission currently supports only single '
                        'values or "*" fields'
                    ),
                    'schedule': schedule_name,
                },
            )

    return issues
