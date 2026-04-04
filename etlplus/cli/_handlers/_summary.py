"""
:mod:`etlplus.cli._handlers._summary` module.

Internal config-summary helpers shared by CLI handlers.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ... import Config
from ...database import load_table_spec
from ...workflow import topological_sort_jobs

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functins
    'check_sections',
    'collect_table_specs',
    'graph_summary',
    'pipeline_summary',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _transform_names(
    cfg: Config,
) -> list[str | None]:
    """Return transform names from mapping- or sequence-based config shapes."""
    if isinstance(cfg.transforms, Mapping):
        return list(cfg.transforms)
    return [getattr(transform, 'name', None) for transform in cfg.transforms]


# SECTION: FUNCTIONS ======================================================== #


def check_sections(
    cfg: Config,
    *,
    jobs: bool,
    pipelines: bool,
    sources: bool,
    targets: bool,
    transforms: bool,
) -> dict[str, Any]:
    """
    Build sectioned metadata output for the check command.

    Parameters
    ----------
    cfg : Config
        The pipeline configuration object.
    jobs : bool
        Whether to include job information.
    pipelines : bool
        Whether to include pipeline information.
    sources : bool
        Whether to include source information.
    targets : bool
        Whether to include target information.
    transforms : bool
        Whether to include transform information.

    Returns
    -------
    dict[str, Any]
        A dictionary containing the requested sections of metadata.
    """
    sections: dict[str, Any] = {}
    summary = pipeline_summary(cfg)

    if jobs:
        sections['jobs'] = summary['jobs']
    if pipelines:
        sections['pipelines'] = [cfg.name]
    if sources:
        sections['sources'] = [src.name for src in cfg.sources]
    if targets:
        sections['targets'] = [tgt.name for tgt in cfg.targets]
    if transforms:
        sections['transforms'] = _transform_names(cfg)
    if not sections:
        sections['jobs'] = summary['jobs']
    return sections


def collect_table_specs(
    config_path: str | None,
    spec_path: str | None,
) -> list[dict[str, Any]]:
    """
    Load table schemas from a pipeline config and/or standalone spec.

    Parameters
    ----------
    config_path : str | None
        Path to the pipeline configuration file.
    spec_path : str | None
        Path to the standalone table specification file.

    Returns
    -------
    list[dict[str, Any]]
        A list of table schema dictionaries.
    """
    specs: list[dict[str, Any]] = []

    if spec_path:
        specs.append(dict(load_table_spec(Path(spec_path))))

    if config_path:
        cfg = Config.from_yaml(config_path, substitute=True)
        specs.extend(getattr(cfg, 'table_schemas', []))

    return specs


def pipeline_summary(
    cfg: Config,
) -> dict[str, Any]:
    """
    Return a human-friendly snapshot of a pipeline config.

    Parameters
    ----------
    cfg : Config
        The pipeline configuration object.

    Returns
    -------
    dict[str, Any]
        A dictionary containing a summary of the pipeline configuration.
    """
    return {
        'name': cfg.name,
        'version': cfg.version,
        'sources': [src.name for src in cfg.sources],
        'targets': [tgt.name for tgt in cfg.targets],
        'jobs': [job.name for job in cfg.jobs],
    }


def graph_summary(
    cfg: Config,
) -> dict[str, Any]:
    """
    Return one dependency-graph summary for configured jobs.

    Parameters
    ----------
    cfg : Config
        The pipeline configuration object.

    Returns
    -------
    dict[str, Any]
        JSON-serializable DAG summary including ordered jobs and dependencies.
    """
    seen: set[str] = set()
    for job in cfg.jobs:
        if job.name in seen:
            raise ValueError(f'Duplicate job name: {job.name}')
        seen.add(job.name)

    ordered_jobs = topological_sort_jobs(cfg.jobs)
    return {
        'job_count': len(ordered_jobs),
        'name': cfg.name,
        'ordered_jobs': [job.name for job in ordered_jobs],
        'status': 'ok',
        'version': cfg.version,
        'jobs': [
            {
                'depends_on': list(job.depends_on),
                'name': job.name,
            }
            for job in ordered_jobs
        ],
    }
