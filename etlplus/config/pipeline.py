"""
etlplus.config.pipeline module.

Pipeline configuration model and helpers for job orchestration.

Notes
-----
- Loads from dicts or YAML and builds typed models for sources, targets, and
    jobs.
- Connector parsing is unified (``parse_connector``) and tolerant; unknown or
    malformed entries are skipped.
- Optional variable substitution merges ``profile.env`` (lower precedence)
    with the provided/environment variables (higher precedence).
"""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any
from typing import Self

from ..file import read_yaml
from .api import ApiConfig
from .connector import parse_connector
from .jobs import ExtractRef
from .jobs import JobConfig
from .jobs import LoadRef
from .jobs import TransformRef
from .jobs import ValidationRef
from .profile import ProfileConfig
from .types import Connector
from .utils import deep_substitute


# SECTION: EXPORTS ========================================================== #


__all__ = ['PipelineConfig', 'load_pipeline_config']


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _build_jobs(
    raw: Mapping[str, Any],
) -> list[JobConfig]:
    """
    Return a list of ``JobConfig`` objects parsed from the mapping.

    Parameters
    ----------
    raw : Mapping[str, Any]
        Raw pipeline mapping.

    Returns
    -------
    list[JobConfig]
        Parsed job configurations.
    """
    jobs: list[JobConfig] = []
    for j in (raw.get('jobs', []) or []):
        if not isinstance(j, dict):
            continue
        name = j.get('name')
        if not isinstance(name, str):
            continue
        # Extract
        extract = None
        if (
            isinstance(ex_raw := j.get('extract') or {}, dict)
            and ex_raw.get('source')
        ):
            extract = ExtractRef(
                source=str(ex_raw.get('source')),
                options=dict(ex_raw.get('options', {}) or {}),
            )
        # Validate
        validate = None
        if (
            isinstance(v_raw := j.get('validate') or {}, dict)
            and v_raw.get('ruleset')
        ):
            validate = ValidationRef(
                ruleset=str(v_raw.get('ruleset')),
                severity=v_raw.get('severity'),
                phase=v_raw.get('phase'),
            )
        # Transform
        transform = None
        if (
            isinstance(tr_raw := j.get('transform') or {}, dict)
            and tr_raw.get('pipeline')
        ):
            transform = TransformRef(pipeline=str(tr_raw.get('pipeline')))
        # Load
        load = None
        if (
            isinstance(ld_raw := j.get('load') or {}, dict)
            and ld_raw.get('target')
        ):
            load = LoadRef(
                target=str(ld_raw.get('target')),
                overrides=dict(ld_raw.get('overrides', {}) or {}),
            )

        jobs.append(
            JobConfig(
                name=name,
                description=j.get('description'),
                extract=extract,
                validate=validate,
                transform=transform,
                load=load,
            ),
        )

    return jobs


def _build_sources(
    raw: Mapping[str, Any],
) -> list[Connector]:
    """
    Return a list of source connectors parsed from the mapping.

    Parameters
    ----------
    raw : Mapping[str, Any]
        Raw pipeline mapping.

    Returns
    -------
    list[Connector]
        Parsed source connectors.
    """
    return _build_connectors(raw, 'sources')


def _build_targets(
    raw: Mapping[str, Any],
) -> list[Connector]:
    """
    Return a list of target connectors parsed from the mapping.

    Parameters
    ----------
    raw : Mapping[str, Any]
        Raw pipeline mapping.

    Returns
    -------
    list[Connector]
        Parsed target connectors.
    """

    return _build_connectors(raw, 'targets')


def _build_connectors(
    raw: Mapping[str, Any],
    key: str,
) -> list[Connector]:
    """
    Return parsed connectors from ``raw[key]`` using tolerant parsing.

    Unknown or malformed entries are skipped to preserve permissiveness.

    Parameters
    ----------
    raw : Mapping[str, Any]
        Raw pipeline mapping.
    key : str
        List-containing top-level key ("sources" or "targets").

    Returns
    -------
    list[Connector]
        Constructed connector instances (malformed entries skipped).
    """
    items: list[Connector] = []
    for obj in (raw.get(key, []) or []):
        if not isinstance(obj, dict):
            continue
        try:
            items.append(parse_connector(obj))
        except TypeError:
            # Skip unsupported types or malformed entries
            continue

    return items


# SECTION: FUNCTIONS ======================================================== #


def load_pipeline_config(
    path: Path | str,
    *,
    substitute: bool = False,
    env: Mapping[str, str] | None = None,
) -> PipelineConfig:
    """
    Load a pipeline YAML file into a ``PipelineConfig`` instance.

    Delegates to ``PipelineConfig.from_yaml`` for construction and optional
    variable substitution.
    """
    return PipelineConfig.from_yaml(path, substitute=substitute, env=env)


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class PipelineConfig:
    """
    Configuration for the data processing pipeline.

    Attributes
    ----------
    name : str | None
        Optional pipeline name.
    version : str | None
        Optional pipeline version string.
    profile : ProfileConfig
        Pipeline profile defaults and environment.
    vars : dict[str, Any]
        Named variables available for substitution.
    apis : dict[str, ApiConfig]
        Named API configurations.
    databases : dict[str, dict[str, Any]]
        Pass-through database config structures.
    file_systems : dict[str, dict[str, Any]]
        Pass-through filesystem config structures.
    sources : list[Connector]
        Source connectors, parsed tolerantly.
    validations : dict[str, dict[str, Any]]
        Validation rule set definitions.
    transforms : dict[str, dict[str, Any]]
        Transform pipeline definitions.
    targets : list[Connector]
        Target connectors, parsed tolerantly.
    jobs : list[JobConfig]
        Job orchestration definitions.
    """

    # -- Attributes -- #

    name: str | None = None
    version: str | None = None
    profile: ProfileConfig = field(default_factory=ProfileConfig)
    vars: dict[str, Any] = field(default_factory=dict)

    apis: dict[str, ApiConfig] = field(default_factory=dict)
    databases: dict[str, dict[str, Any]] = field(default_factory=dict)
    file_systems: dict[str, dict[str, Any]] = field(default_factory=dict)

    sources: list[Connector] = field(default_factory=list)
    validations: dict[str, dict[str, Any]] = field(default_factory=dict)
    transforms: dict[str, dict[str, Any]] = field(default_factory=dict)
    targets: list[Connector] = field(default_factory=list)
    jobs: list[JobConfig] = field(default_factory=list)

    # -- Class Methods -- #

    @classmethod
    def from_yaml(
        cls,
        path: Path | str,
        *,
        substitute: bool = False,
        env: Mapping[str, str] | None = None,
    ) -> Self:
        """
        Parse a YAML file into a ``PipelineConfig`` instance.

        Parameters
        ----------
        path : Path | str
            Path to the YAML file.
        substitute : bool, default False
            Perform variable substitution after initial parse.
        env : Mapping[str, str] | None, default None
            Environment mapping used for substitution; if omitted use
            ``os.environ``.

        Returns
        -------
        PipelineConfig
            Parsed pipeline configuration.
        """
        raw = read_yaml(Path(path))
        if not isinstance(raw, dict):
            raise TypeError('Pipeline YAML must have a mapping/object root')

        cfg = cls.from_dict(raw)

        if substitute:
            # Merge order: profile.env first (lowest), then provided env or
            # os.environ (highest). External env overrides profile defaults.
            base_env = dict(getattr(cfg.profile, 'env', {}) or {})
            external = (
                dict(env) if env is not None else dict(os.environ)
            )
            env_map = base_env | external
            resolved = deep_substitute(raw, cfg.vars, env_map)
            cfg = cls.from_dict(resolved)

        return cfg

    # -- Class Methods -- #

    @classmethod
    def from_dict(
        cls,
        raw: Mapping[str, Any],
    ) -> Self:
        """
        Parse a mapping into a ``PipelineConfig`` instance.

        Parameters
        ----------
        raw : Mapping[str, Any]
            Raw pipeline mapping.

        Returns
        -------
        PipelineConfig
            Parsed pipeline configuration.
        """
        # Basic metadata
        name = raw.get('name')
        version = raw.get('version')

        # Profile and vars
        prof_raw = raw.get('profile', {}) or {}
        profile = ProfileConfig.from_obj(prof_raw)
        vars_map: dict[str, Any] = dict(raw.get('vars', {}) or {})

        # APIs
        apis: dict[str, ApiConfig] = {}
        for api_name, api_obj in (raw.get('apis', {}) or {}).items():
            apis[str(api_name)] = ApiConfig.from_obj(api_obj)

        # Databases and file systems (pass-through structures)
        databases = dict(raw.get('databases', {}) or {})
        file_systems = dict(raw.get('file_systems', {}) or {})

        # Sources
        sources = _build_sources(raw)

        # Validations/Transforms
        validations = dict(raw.get('validations', {}) or {})
        transforms = dict(raw.get('transforms', {}) or {})

        # Targets
        targets = _build_targets(raw)

        # Jobs
        jobs = _build_jobs(raw)

        return cls(
            name=name,
            version=version,
            profile=profile,
            vars=vars_map,
            apis=apis,
            databases=databases,
            file_systems=file_systems,
            sources=sources,
            validations=validations,
            transforms=transforms,
            targets=targets,
            jobs=jobs,
        )
