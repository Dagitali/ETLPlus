"""
:mod:`etlplus.cli._handlers.schedule` module.

Schedule-config inspection helpers for the CLI facade.
"""

from __future__ import annotations

from ... import Config
from . import _output

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'schedule_handler',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _schedule_payload(
    cfg: Config,
) -> dict[str, object]:
    """Return one JSON payload describing configured schedules."""
    schedules: list[dict[str, object]] = []
    for schedule in getattr(cfg, 'schedules', []):
        target = getattr(schedule, 'target', None)
        interval = getattr(schedule, 'interval', None)
        backfill = getattr(schedule, 'backfill', None)
        schedule_payload: dict[str, object] = {
            'name': schedule.name,
            'paused': schedule.paused,
        }
        if isinstance(schedule.cron, str):
            schedule_payload['cron'] = schedule.cron
        if interval is not None:
            schedule_payload['interval'] = {
                'minutes': interval.minutes,
            }
        if isinstance(schedule.timezone, str):
            schedule_payload['timezone'] = schedule.timezone
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
    pretty: bool = True,
) -> int:
    """
    List configured portable schedules from one ETLPlus config.

    Parameters
    ----------
    config : str
        Path to the ETLPlus config YAML file.
    pretty : bool, optional
        Whether to pretty-print the output JSON. Defaults to ``True``.

    Returns
    -------
    int
        Exit code (0 for success).
    """
    cfg = Config.from_yaml(config, substitute=True)
    return _output.emit_json_payload(_schedule_payload(cfg), pretty=pretty)
