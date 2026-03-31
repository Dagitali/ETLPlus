"""
:mod:`etlplus.cli._handler_check` module.

Config inspection helpers for the CLI facade.
"""

from __future__ import annotations

from .. import Config
from . import _handler_output as _output
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
    pretty : bool, optional
         Whether to pretty-print the JSON output. Default is ``True``.

    Returns
    -------
    int
        Exit code indicating success (``0``) or failure (non-zero).
    """
    if readiness:
        return _output.emit_readiness_report(config=config, pretty=pretty)
    if config is None:
        raise ValueError('config is required unless readiness-only mode is used')

    cfg = Config.from_yaml(config, substitute=substitute)
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
