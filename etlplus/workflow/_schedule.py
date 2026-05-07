"""
:mod:`etlplus.workflow._schedule` module.

Data classes modeling portable schedule configuration.
"""

from __future__ import annotations

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
]


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
