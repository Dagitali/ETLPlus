"""
:mod:`etlplus.cli._handlers.check` module.

Config inspection helpers for the CLI facade.
"""

from __future__ import annotations

from ... import Config
from ...runtime import ReadinessReportBuilder
from . import _output
from . import _summary

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'check_handler',
]


# SECTION: FUNCTIONS ======================================================== #


def check_handler(
    *,
    config: str | None = None,
    graph: bool = False,
    jobs: bool = False,
    pipelines: bool = False,
    readiness: bool = False,
    sources: bool = False,
    strict: bool = False,
    summary: bool = False,
    targets: bool = False,
    transforms: bool = False,
    substitute: bool = True,
    pretty: bool = True,
) -> int:
    """
    Inspect requested specification sections of an ETL job/pipeline
    configuration.

    Parameters
    ----------
    config : str | None, optional
        Optional path to the config file. Default is ``None``.
    graph : bool, optional
        Whether to validate job dependencies and print DAG execution order.
        Default is ``False``.
    jobs : bool, optional
        Whether to print job specs. Default is ``False``.
    pipelines : bool, optional
        Whether to print pipeline specs. Default is ``False``.
    readiness : bool, optional
        Whether to emit a readiness report instead of spec sections. Default is
        ``False``.
    sources : bool, optional
        Whether to print source specs. Default is ``False``.
    strict : bool, optional
        Whether to enable strict config diagnostics. Default is ``False``.
    summary : bool, optional
        Whether to print a summary of the configuration instead of spec
        sections. Default is ``False``.
    targets : bool, optional
        Whether to print target specs. Default is ``False``.
    transforms : bool, optional
        Whether to print transform specs. Default is ``False``.
    substitute : bool, optional
        Whether to perform variable substitution in the config. Default is
        ``True``.
    pretty : bool, optional
        Whether to pretty-print the JSON output. Default is ``True``.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).

    Raises
    ------
    ValueError
        If the config file is required but not provided.
    """
    if readiness:
        report = ReadinessReportBuilder.build(
            config_path=config,
            strict=strict,
        )
        return _output.emit_json_payload(
            report,
            pretty=pretty,
            exit_code=1 if report.get('status') == 'error' else 0,
        )
    if config is None:
        raise ValueError('config is required unless readiness-only mode is used')

    if strict:
        report = ReadinessReportBuilder.strict_config_report(config_path=config)
        if report.get('status') == 'error':
            return _output.emit_json_payload(
                report,
                pretty=pretty,
                exit_code=1,
            )

    cfg = Config.from_yaml(config, substitute=substitute)
    if graph:
        try:
            return _output.emit_json_payload(
                _summary.graph_summary(cfg),
                pretty=pretty,
            )
        except ValueError as exc:
            return _output.emit_json_payload(
                {'message': str(exc), 'status': 'error'},
                pretty=pretty,
                exit_code=1,
            )
    payload = (
        _summary.pipeline_summary(cfg)
        if summary
        else _summary.check_sections(
            cfg,
            jobs=jobs,
            pipelines=pipelines,
            sources=sources,
            targets=targets,
            transforms=transforms,
        )
    )
    return _output.emit_json_payload(payload, pretty=pretty)
