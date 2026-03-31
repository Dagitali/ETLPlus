"""
:mod:`etlplus.cli._handler_run` module.

Run-command implementation for the CLI facade.
"""

from __future__ import annotations

from typing import cast

from .. import Config
from .. import __version__
from ..history import HistoryStore
from ..history import build_run_record
from ..ops import run
from ..utils._types import JSONData
from . import _handler_lifecycle as _lifecycle
from . import _handler_output as _output
from . import _summary

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
    Execute an ETL job end-to-end from a YAML configuration.

    Parameters
    ----------
    config : str
        Path to the YAML configuration file for the run.
    job : str | None, optional
        The name of the job to run, if the configuration defines multiple jobs.
    pipeline : str | None, optional
        The name of the pipeline to run, if the configuration defines multiple
        pipelines. If both *job* and *pipeline* are provided, *job* takes
        precedence.
    event_format : str | None, optional
        The format of the events to emit during the run.
    pretty : bool, optional
        Whether to pretty-print the output.

    Returns
    -------
    int
        The exit code for the command, typically 0 for success or non-zero for
        failure.
    """
    cfg = Config.from_yaml(config, substitute=True)

    job_name = job or pipeline
    if not job_name:
        return _output.emit_json_payload(_summary.pipeline_summary(cfg), pretty=pretty)

    context = _lifecycle.start_command(
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

    with _lifecycle.failure_boundary(
        context,
        on_error=lambda exc: _lifecycle.record_run_completion(
            history_store,
            context,
            status='failed',
            exc=exc,
        ),
        config_path=config,
        job=job_name,
        pipeline_name=cfg.name,
    ):
        result = run(job=job_name, config_path=config)

    _lifecycle.record_run_completion(
        history_store,
        context,
        status='succeeded',
        result_summary=cast(JSONData | None, result),
    )
    result_status = result.get('status') if isinstance(result, dict) else None
    return _output.complete_output(
        context,
        {'run_id': context.run_id, 'status': 'ok', 'result': result},
        mode='json',
        pretty=pretty,
        config_path=config,
        job=job_name,
        pipeline_name=cfg.name,
        result_status=result_status,
        status='ok',
    )
