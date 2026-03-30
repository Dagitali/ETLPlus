"""
:mod:`etlplus.cli._handler_check` module.

Config-inspection handler implementation for the CLI facade.
"""

from __future__ import annotations

from typing import Any

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
    config_cls: Any,
    emit_json_payload_fn: Any,
    emit_readiness_report_fn: Any,
    check_sections_fn: Any,
    pipeline_summary_fn: Any,
) -> int:
    """Print requested pipeline sections from a YAML configuration."""
    if readiness:
        return emit_readiness_report_fn(config=config, pretty=pretty)

    if config is None:
        raise ValueError('config is required unless readiness-only mode is used')

    cfg = config_cls.from_yaml(config, substitute=substitute)
    if summary:
        return emit_json_payload_fn(pipeline_summary_fn(cfg), pretty=pretty)

    return emit_json_payload_fn(
        check_sections_fn(
            cfg,
            jobs=jobs,
            pipelines=pipelines,
            sources=sources,
            targets=targets,
            transforms=transforms,
        ),
        pretty=pretty,
    )
