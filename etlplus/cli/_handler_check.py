"""
:mod:`etlplus.cli._handler_check` module.

Config inspection helpers for the CLI facade.
"""

from __future__ import annotations

from typing import Any

from .. import Config
from . import _handler_common as _common_impl

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'check_handler',
]


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
    check_sections: Any | None = None,
    pipeline_summary: Any | None = None,
    config_cls: Any = Config,
    emit_json_payload_fn: Any = _common_impl.emit_json_payload,
    emit_readiness_report_fn: Any = _common_impl.emit_readiness_report,
    check_sections_fn: Any | None = None,
    pipeline_summary_fn: Any | None = None,
) -> int:
    """
    Print requested pipeline sections from a YAML configuration.

    Parameters
    ----------
    config : str | None, optional
        Optional path to a config file containing pipeline specs. Default is
        ``None``.
    jobs : bool, optional
        Whether to print job specs. Default is ``False``.
    pipelines : bool, optional
        Whether to print pipeline specs. Default is ``False``.
    readiness : bool, optional
        Whether to emit a readiness report instead of pipeline specs. Default
        is ``False``.
    sources : bool, optional
        Whether to print source specs. Default is ``False``.
    summary : bool, optional
        Whether to print a summary of the pipeline configuration instead of
        detailed specs. Default is ``False``.
    targets : bool, optional
        Whether to print target specs. Default is ``False``.
    transforms : bool, optional
        Whether to print transform specs. Default is ``False``.
    substitute : bool, optional
        Whether to perform variable substitution in the config. Default is
        ``True``.
    check_sections : Any | None, optional
        Optional function to retrieve check sections from the config. If not
        provided, a default implementation will be used. Default is ``None``.
    pipeline_summary : Any | None, optional
        Optional function to retrieve a summary of the pipeline configuration.
        If not provided, a default implementation will be used. Default is
        ``None``.
    config_cls : Any, optional
        The configuration class to use for parsing the config file. Default is
        :class:`Config`.
    emit_json_payload_fn : Any, optional
        Function to emit a JSON payload. Default is
        :func:`_common_impl.emit_json_payload`.
    emit_readiness_report_fn : Any, optional
        Function to emit a readiness report. Default is
        :func:`_common_impl.emit_readiness_report`.
    check_sections_fn : Any | None, optional
        Optional function to retrieve check sections from the config. If not
        provided, a default implementation will be used. Default is ``None``.
    pipeline_summary_fn : Any | None, optional
        Optional function to retrieve a summary of the pipeline configuration.
        If not provided, a default implementation will be used. Default is
        ``None``.

    Returns
    -------
    int
        Exit code indicating success (``0``) or failure (non-zero).
    """
    resolved_check_sections = (
        check_sections if check_sections is not None else check_sections_fn
    )
    resolved_pipeline_summary = (
        pipeline_summary if pipeline_summary is not None else pipeline_summary_fn
    )

    if readiness:
        return emit_readiness_report_fn(config=config, pretty=pretty)
    if config is None:
        raise ValueError('config is required unless readiness-only mode is used')
    if resolved_check_sections is None or resolved_pipeline_summary is None:
        raise ValueError('check_sections and pipeline_summary are required')

    cfg = config_cls.from_yaml(config, substitute=substitute)
    payload = (
        resolved_pipeline_summary(cfg)
        if summary
        else resolved_check_sections(
            cfg,
            jobs=jobs,
            pipelines=pipelines,
            sources=sources,
            targets=targets,
            transforms=transforms,
        )
    )
    return emit_json_payload_fn(payload, pretty=pretty)
