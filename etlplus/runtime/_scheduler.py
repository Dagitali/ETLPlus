"""
:mod:`etlplus.runtime._scheduler` module.

Local schedule-trigger execution helpers.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Literal
from zoneinfo import ZoneInfo
from zoneinfo import ZoneInfoNotFoundError

from ..history._config import ResolvedHistoryConfig
from ..utils import JsonCodec

if TYPE_CHECKING:
    from collections.abc import Callable

    from .. import Config


# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'LocalScheduler',
    'ScheduledRunRequest',
]


# SECTION: CONSTANTS ======================================================== #


_SCHEDULER_STATE_FILE = 'scheduler-state.json'
_SCHEDULER_LOCK_DIR = 'scheduler-locks'
_MINUTE = timedelta(minutes=1)


# SECTION: INTERNAL DATA CLASSES ============================================ #


@dataclass(slots=True, frozen=True)
class _ResolvedSchedule:
    """Normalized internal schedule view for dispatch decisions."""

    # -- Instance Attributes -- #

    job_name: str | None
    name: str
    paused: bool
    run_all: bool
    trigger: Literal['cron', 'interval']


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True, frozen=True)
class ScheduledRunRequest:
    """One due schedule execution request."""

    # -- Instance Attributes -- #

    catchup: bool
    job_name: str | None
    run_all: bool
    schedule_name: str
    trigger: str
    triggered_at: str


# SECTION: INTERNAL CLASSES ================================================= #


class _SchedulerStateStore:
    """Small JSON-backed store for scheduler trigger state."""

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(
        self,
        state_dir: Path,
    ) -> None:
        self._state_dir = state_dir
        self._state_file = state_dir / _SCHEDULER_STATE_FILE

    # -- Internal Instance Methods -- #

    def _load(self) -> dict[str, dict[str, str]]:
        if not self._state_file.exists():
            return {}
        data = JsonCodec.parse(self._state_file.read_text(encoding='utf-8'))
        if not isinstance(data, dict):
            return {}
        schedules = data.get('schedules')
        if not isinstance(schedules, dict):
            return {}
        return {
            name: value
            for name, value in schedules.items()
            if isinstance(name, str) and isinstance(value, dict)
        }

    def _save(
        self,
        schedules: dict[str, dict[str, str]],
    ) -> None:
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._state_file.write_text(
            JsonCodec().serialize({'schedules': schedules}),
            encoding='utf-8',
        )

    # -- Instance Methods -- #

    def last_triggered_at(
        self,
        schedule_name: str,
    ) -> str | None:
        """
        Return the last completed trigger timestamp for one schedule.

        Parameters
        ----------
        schedule_name : str
            Schedule name to query for the last trigger timestamp.

        Returns
        -------
        str | None
            The last completed trigger timestamp as an ISO-8601 string, or
            ``None`` if no valid record is found.
        """
        return self.last_completed_at(schedule_name)

    def last_completed_at(
        self,
        schedule_name: str,
    ) -> str | None:
        """Return the last completed trigger timestamp for one schedule."""
        schedule_state = self._load().get(schedule_name)
        if not isinstance(schedule_state, dict):
            return None
        value = (
            schedule_state.get('last_completed_at')
            or schedule_state.get('last_triggered_at')
        )
        return value if isinstance(value, str) and value else None

    def record_attempt(
        self,
        *,
        schedule_name: str,
        triggered_at: str,
    ) -> None:
        """Persist one attempted schedule trigger timestamp."""
        schedules = self._load()
        schedule_state = schedules.get(schedule_name, {})
        if not isinstance(schedule_state, dict):
            schedule_state = {}
        schedule_state['last_attempted_at'] = triggered_at
        schedules[schedule_name] = schedule_state
        self._save(schedules)

    def record_completion(
        self,
        *,
        schedule_name: str,
        triggered_at: str,
        status: str,
        run_id: str | None = None,
    ) -> None:
        """Persist one completed schedule trigger timestamp and outcome."""
        schedules = self._load()
        schedule_state = schedules.get(schedule_name, {})
        if not isinstance(schedule_state, dict):
            schedule_state = {}
        schedule_state['last_attempted_at'] = triggered_at
        schedule_state['last_completed_at'] = triggered_at
        schedule_state['last_status'] = status
        if run_id is None:
            schedule_state.pop('last_run_id', None)
        else:
            schedule_state['last_run_id'] = run_id
        schedules[schedule_name] = schedule_state
        self._save(schedules)

    def record_trigger(
        self,
        *,
        schedule_name: str,
        triggered_at: str,
    ) -> None:
        """
        Persist one schedule trigger timestamp.

        Parameters
        ----------
        schedule_name : str
            Schedule name to record the trigger timestamp for.
        triggered_at : str
            Trigger timestamp as an ISO-8601 string to record for the schedule.
        """
        self.record_completion(
            schedule_name=schedule_name,
            triggered_at=triggered_at,
            status='ok',
        )


class _ScheduleLock:
    """Best-effort filesystem lock for one schedule name."""

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(
        self,
        state_dir: Path,
        schedule_name: str,
    ) -> None:
        lock_dir = state_dir / _SCHEDULER_LOCK_DIR
        slug = (
            ''.join(
                ch if ch.isalnum() else '-' for ch in schedule_name.casefold()
            ).strip('-')
            or 'schedule'
        )
        self._lock_path = lock_dir / f'{slug}.lock'
        self._fd: int | None = None

    # -- Instance Methods -- #

    def acquire(self) -> bool:
        """Return whether the schedule lock was acquired."""
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._fd = os.open(
                self._lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o644,
            )
        except FileExistsError:
            return False
        os.write(self._fd, str(os.getpid()).encode('utf-8'))
        return True

    def release(self) -> None:
        """Release the acquired schedule lock when present."""
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        self._lock_path.unlink(missing_ok=True)


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _backfill_limit(
    schedule: object,
) -> int:
    """Return the bounded replay limit for one schedule."""
    backfill = getattr(schedule, 'backfill', None)
    if not getattr(backfill, 'enabled', False):
        return 1
    limit = getattr(backfill, 'max_catchup_runs', None)
    return limit if isinstance(limit, int) and limit > 0 else 1


def _cron_field_matches(
    field: str,
    value: int,
) -> bool:
    """Return whether one simple cron field matches *value*."""
    if field == '*':
        return True
    try:
        return int(field) == value
    except ValueError:
        return False


def _cron_matches(
    schedule: object,
    when_local: datetime,
) -> bool:
    """Return whether *when_local* satisfies one schedule cron."""
    cron = getattr(schedule, 'cron', None)
    if not isinstance(cron, str) or not cron:
        return False
    fields = cron.split()
    if len(fields) != 5:
        return False
    minute, hour, day, month, weekday = fields
    cron_weekday = (when_local.weekday() + 1) % 7
    return all(
        (
            _cron_field_matches(minute, when_local.minute),
            _cron_field_matches(hour, when_local.hour),
            _cron_field_matches(day, when_local.day),
            _cron_field_matches(month, when_local.month),
            _cron_field_matches(weekday, cron_weekday)
            or (cron_weekday == 0 and weekday == '7'),
        ),
    )


def _floor_to_minute(
    value: datetime,
) -> datetime:
    """Return *value* truncated to minute precision."""
    return value.replace(second=0, microsecond=0)


def _parse_timestamp(
    value: str | None,
) -> datetime | None:
    """Parse one ISO-8601 timestamp into an aware datetime."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _resolve_schedule(
    schedule: object,
) -> _ResolvedSchedule | None:
    """Return one normalized schedule shape or ``None`` when incomplete."""
    name = getattr(schedule, 'name', None)
    target = getattr(schedule, 'target', None)
    if not isinstance(name, str) or target is None:
        return None

    raw_job_name = getattr(target, 'job', None)
    return _ResolvedSchedule(
        job_name=raw_job_name if isinstance(raw_job_name, str) else None,
        name=name,
        paused=bool(getattr(schedule, 'paused', False)),
        run_all=bool(getattr(target, 'run_all', False)),
        trigger=(
            'interval' if getattr(schedule, 'interval', None) is not None else 'cron'
        ),
    )


def _resolve_timezone(
    schedule: object,
) -> ZoneInfo:
    """Return the configured schedule timezone or UTC."""
    timezone_name = getattr(schedule, 'timezone', None)
    if not isinstance(timezone_name, str) or not timezone_name:
        return ZoneInfo('UTC')
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return ZoneInfo('UTC')


def _schedule_metadata(
    request: ScheduledRunRequest,
    *,
    status: str,
    reason: str | None = None,
    run_id: str | None = None,
) -> dict[str, object]:
    """Return one summarized scheduler result row."""
    payload: dict[str, object] = {
        'catchup': request.catchup,
        'schedule': request.schedule_name,
        'status': status,
        'trigger': request.trigger,
        'triggered_at': request.triggered_at,
    }
    if request.job_name is not None:
        payload['job'] = request.job_name
    if request.run_all:
        payload['run_all'] = True
    if reason is not None:
        payload['reason'] = reason
    if run_id is not None:
        payload['run_id'] = run_id
    return payload


def _iter_matching_schedules(
    cfg: Config,
    *,
    schedule_name: str | None,
) -> tuple[tuple[object, _ResolvedSchedule], ...]:
    """Return normalized schedules filtered to one optional schedule name."""
    return tuple(
        (schedule, resolved)
        for schedule in getattr(cfg, 'schedules', [])
        if (resolved := _resolve_schedule(schedule)) is not None
        and (schedule_name is None or resolved.name == schedule_name)
    )


# SECTION: CLASSES ========================================================== #


class LocalScheduler:
    """Local due-schedule dispatcher built on top of ``etlplus run``."""

    # -- Class Methods -- #

    @classmethod
    def due_requests(
        cls,
        cfg: Config,
        *,
        now: datetime,
        schedule_name: str | None,
        state_store: _SchedulerStateStore,
    ) -> tuple[list[ScheduledRunRequest], list[dict[str, object]]]:
        """Return due schedule requests and skipped rows for paused schedules."""
        requests: list[ScheduledRunRequest] = []
        skipped: list[dict[str, object]] = []
        current_minute_utc = _floor_to_minute(now.astimezone(UTC))

        for schedule, resolved in _iter_matching_schedules(
            cfg,
            schedule_name=schedule_name,
        ):
            if resolved.paused:
                skipped.append(
                    _schedule_metadata(
                        ScheduledRunRequest(
                            catchup=False,
                            job_name=resolved.job_name,
                            run_all=resolved.run_all,
                            schedule_name=resolved.name,
                            trigger=resolved.trigger,
                            triggered_at=now.isoformat(),
                        ),
                        status='skipped',
                        reason='paused',
                    ),
                )
                continue

            due_times = (
                cls._interval_due_times(
                    schedule,
                    now=now,
                    previous_triggered_at=state_store.last_completed_at(resolved.name),
                )
                if resolved.trigger == 'interval'
                else cls._cron_due_times(
                    schedule,
                    now=now,
                    previous_triggered_at=state_store.last_completed_at(resolved.name),
                )
            )
            for due_time in due_times:
                triggered_at = due_time.astimezone(UTC)
                requests.append(
                    ScheduledRunRequest(
                        catchup=triggered_at < current_minute_utc,
                        job_name=resolved.job_name,
                        run_all=resolved.run_all,
                        schedule_name=resolved.name,
                        trigger=resolved.trigger,
                        triggered_at=triggered_at.isoformat(),
                    ),
                )
        return requests, skipped

    @classmethod
    def run_pending(
        cls,
        *,
        cfg: Config,
        config_path: str,
        event_format: str | None,
        pretty: bool,
        run_callback: Callable[..., int],
        schedule_name: str | None = None,
        state_dir: str | os.PathLike[str] | None = None,
    ) -> dict[str, object]:
        """Dispatch due schedules once and return a summary payload."""
        settings = ResolvedHistoryConfig.resolve(
            getattr(cfg, 'history', None),
            env=os.environ,
            state_dir=state_dir,
        )
        state_store = _SchedulerStateStore(settings.state_dir)
        now = cls.utc_now()
        requests, skipped = cls.due_requests(
            cfg,
            now=now,
            schedule_name=schedule_name,
            state_store=state_store,
        )
        schedule_count = len(
            _iter_matching_schedules(cfg, schedule_name=schedule_name),
        )

        results: list[dict[str, object]] = list(skipped)
        dispatched_count = 0
        for request in requests:
            lock = _ScheduleLock(settings.state_dir, request.schedule_name)
            if not lock.acquire():
                results.append(
                    _schedule_metadata(
                        request,
                        status='skipped',
                        reason='overlap',
                    ),
                )
                continue
            try:
                state_store.record_attempt(
                    schedule_name=request.schedule_name,
                    triggered_at=request.triggered_at,
                )
                captured: dict[str, object] = {}
                exit_code = run_callback(
                    config=config_path,
                    event_format=event_format,
                    job=request.job_name,
                    pretty=pretty,
                    result_recorder=captured.update,
                    run_all=request.run_all,
                    schedule_catchup=request.catchup,
                    schedule_name=request.schedule_name,
                    schedule_trigger=request.trigger,
                    schedule_triggered_at=request.triggered_at,
                    emit_output=False,
                )
            finally:
                lock.release()

            dispatched_count += 1
            raw_run_id = captured.get('run_id')
            run_id: str | None = raw_run_id if isinstance(raw_run_id, str) else None
            status = 'ok' if exit_code == 0 else 'error'
            state_store.record_completion(
                schedule_name=request.schedule_name,
                triggered_at=request.triggered_at,
                status=status,
                run_id=run_id,
            )
            results.append(
                _schedule_metadata(
                    request,
                    status=status,
                    run_id=run_id,
                ),
            )

        return {
            'checked_at': now.isoformat(),
            'dispatched_count': dispatched_count,
            'name': cfg.name,
            'run_count': len(results),
            'schedule_count': schedule_count,
            'runs': results,
            'skipped_count': len(
                [row for row in results if row.get('status') == 'skipped'],
            ),
        }

    # -- Internal Static Methods -- #

    @staticmethod
    def _cron_due_times(
        schedule: object,
        *,
        now: datetime,
        previous_triggered_at: str | None,
    ) -> list[datetime]:
        """Return due cron trigger times for one schedule."""
        timezone = _resolve_timezone(schedule)
        current_local = _floor_to_minute(now.astimezone(timezone))
        previous = _parse_timestamp(previous_triggered_at)
        if previous is not None:
            earliest = _floor_to_minute(previous.astimezone(timezone) + _MINUTE)
        else:
            backfill = getattr(schedule, 'backfill', None)
            earliest = _floor_to_minute(current_local)
            if getattr(backfill, 'enabled', False):
                start_at = _parse_timestamp(getattr(backfill, 'start_at', None))
                if start_at is not None:
                    earliest = _floor_to_minute(start_at.astimezone(timezone))

        matches: list[datetime] = []
        cursor = current_local
        while cursor >= earliest and len(matches) < _backfill_limit(schedule):
            if _cron_matches(schedule, cursor):
                matches.append(cursor)
            cursor -= _MINUTE
        matches.reverse()
        return matches

    @staticmethod
    def _interval_due_times(
        schedule: object,
        *,
        now: datetime,
        previous_triggered_at: str | None,
    ) -> list[datetime]:
        """Return due interval trigger times for one schedule."""
        interval = getattr(schedule, 'interval', None)
        minutes = getattr(interval, 'minutes', None)
        if not isinstance(minutes, int) or minutes < 1:
            return []
        timezone = _resolve_timezone(schedule)
        current_local = now.astimezone(timezone)
        step = timedelta(minutes=minutes)
        previous = _parse_timestamp(previous_triggered_at)
        if previous is not None:
            next_due = previous.astimezone(timezone) + step
            due_times: list[datetime] = []
            while next_due <= current_local and len(due_times) < _backfill_limit(
                schedule,
            ):
                due_times.append(next_due)
                next_due += step
            return due_times

        backfill = getattr(schedule, 'backfill', None)
        if getattr(backfill, 'enabled', False):
            start_at = _parse_timestamp(getattr(backfill, 'start_at', None))
            if start_at is not None:
                due_times = []
                cursor = start_at.astimezone(timezone)
                while cursor <= current_local and len(due_times) < _backfill_limit(
                    schedule,
                ):
                    due_times.append(cursor)
                    cursor += step
                return due_times

        return [current_local]

    # -- Static Methods -- #

    @staticmethod
    def utc_now() -> datetime:
        """Return the current UTC time."""
        return datetime.now(UTC)
