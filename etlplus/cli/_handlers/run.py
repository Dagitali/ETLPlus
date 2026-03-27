"""
:mod:`etlplus.cli._handlers.run` module.

Run-command handler.
"""

from __future__ import annotations

from typing import cast

from ... import Config
from ... import __version__
from ...history import HistoryStore
from ...history import RunCompletion
from ...history import RunState
from ...history import build_run_record
from ...ops import run
from ...runtime.events import RuntimeEvents
from ...utils.types import JSONData
from .._summary import pipeline_summary as _pipeline_summary
from .common import _complete_command
from .common import _elapsed_ms
from .common import _emit_json_payload
from .common import _fail_command
from .common import _start_command

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'run_handler',
]


# SECTION: FUNCTIONS ======================================================== #


def run_handler(
    *,
    config: str,
    job: str | None = None,
    pipeline: str | None = None,
    event_format: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Execute an ETL job end-to-end from a pipeline YAML configuration.

    Parameters
    ----------
    config : str
        Path to the pipeline YAML configuration to run.
    job : str | None, optional
        Optional name of the job to run (if not specified, the entire pipeline
        will be run).
    pipeline : str | None, optional
        Optional name of the pipeline to run (if not specified, the pipeline
        name from the config will be used in emitted events and history records).
    event_format : str | None, optional
        Format of structured events to emit during the run (if not specified,
        events will be emitted in a default format).
    pretty : bool, optional
        Whether to pretty-print JSON output.

    Returns
    -------
    int
        Exit code (0 if the run succeeded, non-zero if any errors occurred).

    Raises
    ------
    Exception
        If any error occurs during the run.
    """
    cfg = Config.from_yaml(config, substitute=True)

    job_name = job or pipeline
    if job_name:
        context = _start_command(
            command='run',
            event_format=event_format,
            config_path=config,
            etlplus_version=__version__,
            job=job_name,
            pipeline_name=cfg.name,
            status='running',
        )
        history_store = HistoryStore.from_environment()
        history_store.record_run_started(
            build_run_record(
                run_id=context.run_id,
                config_path=config,
                started_at=context.started_at,
                pipeline_name=cfg.name,
                job_name=job_name,
            ),
        )
        try:
            result = run(job=job_name, config_path=config)
        except Exception as exc:
            duration_ms = _elapsed_ms(context.started_perf)
            history_store.record_run_finished(
                RunCompletion(
                    run_id=context.run_id,
                    state=RunState(
                        status='failed',
                        finished_at=RuntimeEvents.utc_now_iso(),
                        duration_ms=duration_ms,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    ),
                ),
            )
            _fail_command(
                context,
                exc,
                config_path=config,
                job=job_name,
                pipeline_name=cfg.name,
            )
            raise

        duration_ms = _elapsed_ms(context.started_perf)
        history_store.record_run_finished(
            RunCompletion(
                run_id=context.run_id,
                state=RunState(
                    status='succeeded',
                    finished_at=RuntimeEvents.utc_now_iso(),
                    duration_ms=duration_ms,
                    result_summary=cast(JSONData | None, result),
                ),
            ),
        )
        _complete_command(
            context,
            config_path=config,
            job=job_name,
            pipeline_name=cfg.name,
            result_status=result.get('status'),
            status='ok',
        )
        return _emit_json_payload(
            {'run_id': context.run_id, 'status': 'ok', 'result': result},
            pretty=pretty,
        )

    return _emit_json_payload(_pipeline_summary(cfg), pretty=pretty)
