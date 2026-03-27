"""
:mod:`etlplus.cli._handlers.check` module.

Check-command helpers and handler.
"""

from __future__ import annotations

from ... import Config
from ...runtime import ReadinessReportBuilder
from .._summary import check_sections as _check_sections
from .._summary import pipeline_summary as _pipeline_summary
from .common import _emit_json_payload

# SECTION: EXPORTS ========================================================== #

__all__ = [
    'check_handler',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _emit_readiness_report(*, config: str | None, pretty: bool) -> int:
    """Build and emit one readiness report, returning its CLI exit code."""
    report = ReadinessReportBuilder.build(config_path=config)
    return _emit_json_payload(
        report,
        pretty=pretty,
        exit_code=0 if report.get('status') == 'ok' else 1,
    )


# SECTION: FUNCTIONS ======================================================== #


def check_handler(
    *,
    config: str | None = None,
    jobs: bool = False,
    pipelines: bool = False,
    readiness: bool = False,
    sources: bool = False,
    summary: bool = False,
    targets: bool = False,
    transforms: bool = False,
    substitute: bool = True,
    pretty: bool = True,
) -> int:
    """
    Print requested sections from a YAML configuration.

    If ``readiness`` is ``True``, only a readiness report will be emitted, and
    the exit code will be ``0`` if the report's status is "ok" and ``1``
    otherwise. In this mode, the ``config`` argument is optional since the
    report builder can often infer a config path from the current working
    directory. In all other cases, a config path must be provided, and the exit
    code will be ``0``.

    Parameters
    ----------
    config : str | None, optional
        Path to YAML/JSON config file. Optional if only a readiness report is
        requested and the builder can infer a config path from the current
        working directory.
    jobs : bool, optional
        Whether to inspect job definitions.
    pipelines : bool, optional
        Whether to inspect pipeline definitions.
    readiness : bool, optional
        Whether to perform a readiness check. If ``True``, no other inspection
        flags may be ``True``.
    sources : bool, optional
        Whether to inspect source definitions.
    summary : bool, optional
        Whether to print a summary of the configuration.
    targets : bool, optional
        Whether to inspect target definitions.
    transforms : bool, optional
        Whether to inspect transform definitions.
    substitute : bool, optional
        Whether to perform environment variable substitution when loading the config.
    pretty : bool, optional
        Whether to pretty-print JSON output.

    Returns
    -------
    int
        Exit code (0 if checks passed or if only inspection was requested, 1 if
        readiness check failed).

    Raises
    ------
    ValueError
        If the provided options are invalid (e.g. if *readiness`* is ``True``
        but the readiness report builder cannot infer a config path, or if
        `*readiness`* is ``True`` but other inspection flags are also
        ``True``).
    """
    if readiness:
        return _emit_readiness_report(config=config, pretty=pretty)

    if config is None:
        raise ValueError('config is required unless readiness-only mode is used')

    cfg = Config.from_yaml(config, substitute=substitute)
    if summary:
        return _emit_json_payload(_pipeline_summary(cfg), pretty=True)

    return _emit_json_payload(
        _check_sections(
            cfg,
            jobs=jobs,
            pipelines=pipelines,
            sources=sources,
            targets=targets,
            transforms=transforms,
        ),
        pretty=pretty,
    )
