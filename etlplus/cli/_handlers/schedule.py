"""
:mod:`etlplus.cli._handlers.schedule` module.

Schedule-config inspection helpers for the CLI facade.
"""

from __future__ import annotations

import re
from pathlib import Path
from shlex import quote as shell_quote

from ... import Config
from ...runtime._scheduler import LocalScheduler
from . import _output
from .run import run_handler as _run_handler

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'schedule_handler',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _cron_field_to_int(
    value: str,
    *,
    label: str,
) -> int:
    """Return one integer cron field or raise a descriptive error."""
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(
            f'{label} must be one integer value or "*" for systemd emission.',
        ) from exc


def _cron_to_on_calendar(
    cron: str,
) -> str:
    """Convert one narrow 5-field cron expression into systemd OnCalendar."""
    fields = cron.split()
    if len(fields) != 5:
        raise ValueError(
            'Cron helper emission currently supports exactly five cron fields.',
        )
    minute, hour, day, month, weekday = fields
    if any(any(token in field for token in (',', '-', '/', '?')) for field in fields):
        raise ValueError(
            'Cron helper emission currently supports only single values or "*" fields.',
        )

    weekday_prefix = ''
    if weekday != '*':
        weekday_names = {
            0: 'Sun',
            1: 'Mon',
            2: 'Tue',
            3: 'Wed',
            4: 'Thu',
            5: 'Fri',
            6: 'Sat',
            7: 'Sun',
        }
        weekday_index = _cron_field_to_int(weekday, label='Cron weekday')
        if weekday_index not in weekday_names:
            raise ValueError('Cron weekday must be 0-7 for systemd emission.')
        weekday_prefix = weekday_names[weekday_index] + ' '

    minute_text = (
        '*'
        if minute == '*'
        else f'{_cron_field_to_int(minute, label="Cron minute"):02d}'
    )
    hour_text = (
        '*' if hour == '*' else f'{_cron_field_to_int(hour, label="Cron hour"):02d}'
    )
    day_text = '*' if day == '*' else str(_cron_field_to_int(day, label='Cron day'))
    month_text = (
        '*' if month == '*' else f'{_cron_field_to_int(month, label="Cron month"):02d}'
    )
    return f'{weekday_prefix}*-{month_text}-{day_text} {hour_text}:{minute_text}:00'


def _resolved_paths(
    config_path: str,
) -> tuple[Path, Path]:
    """Return resolved config path and working directory for helper snippets."""
    resolved_config = Path(config_path).expanduser().resolve()
    return resolved_config, resolved_config.parent


def _resolve_schedule(
    cfg: Config,
    *,
    schedule_name: str | None,
) -> object | None:
    """Return one selected schedule object or ``None`` when unfiltered."""
    schedules = list(getattr(cfg, 'schedules', []))
    if schedule_name is None:
        return None
    for schedule in schedules:
        if getattr(schedule, 'name', None) == schedule_name:
            return schedule
    raise ValueError(f'Schedule not found: {schedule_name}')


def _run_command(
    *,
    config_path: Path,
    schedule: object,
) -> str:
    """Return the ETLPlus run command for one configured schedule target."""
    schedule_name = str(getattr(schedule, 'name', 'schedule'))
    target = getattr(schedule, 'target', None)
    if target is None:
        raise ValueError(
            f'Schedule {schedule_name} must define a target for helper emission.',
        )
    if getattr(target, 'run_all', False):
        return f'etlplus run --config {shell_quote(str(config_path))} --all'
    job_name = getattr(target, 'job', None)
    if isinstance(job_name, str) and job_name:
        return (
            'etlplus run --config '
            f'{shell_quote(str(config_path))} --job {shell_quote(job_name)}'
        )
    raise ValueError(
        f'Schedule {schedule_name} must target one job or run_all for helper emission.',
    )


def _schedule_slug(
    schedule_name: str,
) -> str:
    """Return one filesystem-friendly slug for generated helper names."""
    slug = re.sub(r'[^a-z0-9]+', '-', schedule_name.casefold()).strip('-')
    return slug or 'schedule'


def _systemd_payload(
    *,
    config_path: Path,
    schedule: object,
    working_directory: Path,
) -> dict[str, object]:
    """Return one JSON payload containing systemd helper snippets."""
    schedule_name = getattr(schedule, 'name', 'schedule')
    slug = _schedule_slug(str(schedule_name))
    run_command = _run_command(config_path=config_path, schedule=schedule)
    service_name = f'etlplus-{slug}.service'
    timer_name = f'etlplus-{slug}.timer'
    if isinstance(cron := getattr(schedule, 'cron', None), str) and cron:
        timer_directive = f'OnCalendar={_cron_to_on_calendar(cron)}'
    elif (interval := getattr(schedule, 'interval', None)) is not None:
        timer_directive = f'OnUnitActiveSec={interval.minutes}m'
    else:
        raise ValueError(
            f'Schedule {schedule_name} must define '
            'cron or interval for systemd emission.',
        )

    service_snippet = (
        '[Unit]\n'
        f'Description=ETLPlus schedule {schedule_name}\n\n'
        '[Service]\n'
        'Type=oneshot\n'
        f'WorkingDirectory={working_directory}\n'
        f'ExecStart={run_command}\n'
    )
    timer_snippet = (
        '[Unit]\n'
        f'Description=Run ETLPlus schedule {schedule_name}\n\n'
        '[Timer]\n'
        f'{timer_directive}\n'
        'Persistent=true\n'
        f'Unit={service_name}\n\n'
        '[Install]\n'
        'WantedBy=timers.target\n'
    )
    return {
        'format': 'systemd',
        'schedule': schedule_name,
        'service_name': service_name,
        'timer_name': timer_name,
        'service': service_snippet,
        'timer': timer_snippet,
    }


def _crontab_payload(
    *,
    config_path: Path,
    schedule: object,
    working_directory: Path,
) -> dict[str, object]:
    """Return one JSON payload containing one crontab helper snippet."""
    schedule_name = getattr(schedule, 'name', 'schedule')
    cron = getattr(schedule, 'cron', None)
    if not isinstance(cron, str) or not cron:
        raise ValueError(
            f'Schedule {schedule_name} must define cron for crontab emission.',
        )
    return {
        'format': 'crontab',
        'schedule': schedule_name,
        'snippet': (
            f'{cron} cd {shell_quote(str(working_directory))} '
            f'&& {_run_command(config_path=config_path, schedule=schedule)}'
        ),
    }


def _schedule_emit_payload(
    *,
    config_path: str,
    emit: str,
    schedule: object,
) -> dict[str, object]:
    """Return helper-emission payload for one named schedule."""
    resolved_config, working_directory = _resolved_paths(config_path)
    return (
        _crontab_payload(
            config_path=resolved_config,
            schedule=schedule,
            working_directory=working_directory,
        )
        if emit == 'crontab'
        else _systemd_payload(
            config_path=resolved_config,
            schedule=schedule,
            working_directory=working_directory,
        )
    )


def _schedule_payload(
    cfg: Config,
    *,
    schedule_name: str | None = None,
) -> dict[str, object]:
    """Return one JSON payload describing configured schedules."""
    schedules: list[dict[str, object]] = []
    for schedule in getattr(cfg, 'schedules', []):
        resolved_name = getattr(schedule, 'name', None)
        if not isinstance(resolved_name, str):
            continue
        if schedule_name is not None and resolved_name != schedule_name:
            continue
        target = getattr(schedule, 'target', None)
        interval = getattr(schedule, 'interval', None)
        backfill = getattr(schedule, 'backfill', None)
        schedule_payload: dict[str, object] = {
            'name': resolved_name,
            'paused': bool(getattr(schedule, 'paused', False)),
        }
        if isinstance(cron := getattr(schedule, 'cron', None), str):
            schedule_payload['cron'] = cron
        if interval is not None:
            schedule_payload['interval'] = {
                'minutes': interval.minutes,
            }
        if isinstance(timezone := getattr(schedule, 'timezone', None), str):
            schedule_payload['timezone'] = timezone
        if target is not None:
            target_payload = {
                key: value
                for key, value in {
                    'job': target.job,
                    'run_all': target.run_all if target.run_all else None,
                }.items()
                if value is not None
            }
            if target_payload:
                schedule_payload['target'] = target_payload
        if backfill is not None:
            backfill_payload = {
                key: value
                for key, value in {
                    'enabled': backfill.enabled if backfill.enabled else None,
                    'max_catchup_runs': backfill.max_catchup_runs,
                    'start_at': backfill.start_at,
                }.items()
                if value is not None
            }
            if backfill_payload:
                schedule_payload['backfill'] = backfill_payload
        schedules.append(schedule_payload)

    return {
        'name': cfg.name,
        'schedule_count': len(schedules),
        'schedules': schedules,
    }


# SECTION: FUNCTIONS ======================================================== #


def schedule_handler(
    *,
    config: str,
    event_format: str | None = None,
    emit: str | None = None,
    pretty: bool = True,
    run_pending: bool = False,
    schedule_name: str | None = None,
) -> int:
    """
    List configured portable schedules from one ETLPlus config.

    Parameters
    ----------
    config : str
        Path to the ETLPlus config YAML file.
    event_format : str | None, optional
        Structured event output format forwarded to scheduled runs.
    emit : str | None, optional
        Optional helper format to emit for one named schedule.
    pretty : bool, optional
        Whether to pretty-print the output JSON. Defaults to ``True``.
    run_pending : bool, optional
        Whether to execute due schedules once in local mode.
    schedule_name : str | None, optional
        Optional schedule name filter.

    Returns
    -------
    int
        Exit code (0 for success).
    """
    cfg = Config.from_yaml(config, substitute=True)
    try:
        selected_schedule = _resolve_schedule(cfg, schedule_name=schedule_name)
        payload = (
            LocalScheduler.run_pending(
                cfg=cfg,
                config_path=config,
                event_format=event_format,
                pretty=pretty,
                run_callback=_run_handler,
                schedule_name=schedule_name,
            )
            if run_pending
            else _schedule_payload(cfg, schedule_name=schedule_name)
            if emit is None
            else _schedule_emit_payload(
                config_path=config,
                emit=emit,
                schedule=(selected_schedule if selected_schedule is not None else None),
            )
        )
    except ValueError as exc:
        return _output.emit_json_payload(
            {
                'message': str(exc),
                'status': 'error',
            },
            pretty=pretty,
            exit_code=1,
        )
    return _output.emit_json_payload(payload, pretty=pretty)
