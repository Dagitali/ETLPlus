"""
etlplus.config.pipeline
=======================

A module defining configuration types for ETL job orchestration.
"""
from __future__ import annotations

import os
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any
from typing import Self

from ..file import read_yaml
from .api import ApiConfig
from .jobs import ExtractRef
from .jobs import JobConfig
from .jobs import LoadRef
from .jobs import TransformRef
from .jobs import ValidationRef
from .profile import ProfileConfig
from .sources import SourceApi
from .sources import SourceDb
from .sources import SourceFile
from .targets import TargetApi
from .targets import TargetDb
from .targets import TargetFile
from .types import Source
from .types import Target
from .utils import deep_substitute


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _build_jobs(
    raw: Mapping[str, Any],
) -> list[JobConfig]:
    """
    Build a list of JobConfig objects from the raw configuration.

    Parameters
    ----------
    raw : Mapping[str, Any]
        The raw configuration dictionary.

    Returns
    -------
    list[JobConfig]
        A list of JobConfig objects.
    """

    jobs: list[JobConfig] = []
    for j in (raw.get('jobs', []) or []):
        if not isinstance(j, dict):
            continue
        name = j.get('name')
        if not isinstance(name, str):
            continue
        # Extract
        ex_raw = j.get('extract') or {}
        extract = None
        if isinstance(ex_raw, dict) and ex_raw.get('source'):
            extract = ExtractRef(
                source=str(ex_raw.get('source')),
                options=dict(ex_raw.get('options', {}) or {}),
            )
        # Validate
        v_raw = j.get('validate') or {}
        validate = None
        if isinstance(v_raw, dict) and v_raw.get('ruleset'):
            validate = ValidationRef(
                ruleset=str(v_raw.get('ruleset')),
                severity=v_raw.get('severity'),
                phase=v_raw.get('phase'),
            )
        # Transform
        tr_raw = j.get('transform') or {}
        transform = None
        if isinstance(tr_raw, dict) and tr_raw.get('pipeline'):
            transform = TransformRef(pipeline=str(tr_raw.get('pipeline')))
        # Load
        ld_raw = j.get('load') or {}
        load = None
        if isinstance(ld_raw, dict) and ld_raw.get('target'):
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
) -> list[Source]:
    """
    Build a list of Source objects from the raw configuration.

    Parameters
    ----------
    raw : Mapping[str, Any]
        The raw configuration dictionary.

    Returns
    -------
    list[Source]
        A list of Source objects.
    """

    sources: list[Source] = []
    type_map: dict[str, Callable[[Mapping[str, Any]], Source]] = {
        'file': SourceFile.from_obj,
        'database': SourceDb.from_obj,
        'api': SourceApi.from_obj,
    }

    for s in (raw.get('sources', []) or []):
        if not isinstance(s, dict):
            continue
        stype = str(s.get('type', '')).casefold()
        name = s.get('name')
        if not isinstance(name, str):
            continue
        build = type_map.get(stype)
        if build is None:
            continue
        sources.append(build(s))

    return sources


def _build_targets(
    raw: Mapping[str, Any],
) -> list[Target]:
    """
    Build a list of Target objects from the raw configuration.

    Parameters
    ----------
    raw : Mapping[str, Any]
        The raw configuration dictionary.

    Returns
    -------
    list[Target]
        A list of Target objects.
    """

    targets: list[Target] = []
    type_map: dict[str, Callable[[Mapping[str, Any]], Target]] = {
        'file': TargetFile.from_obj,
        'api': TargetApi.from_obj,
        'database': TargetDb.from_obj,
    }

    for t in (raw.get('targets', []) or []):
        if not isinstance(t, dict):
            continue
        ttype = str(t.get('type', '')).casefold()
        name = t.get('name')
        if not isinstance(name, str):
            continue
        build = type_map.get(ttype)
        if build is None:
            continue
        targets.append(build(t))

    return targets


# SECTION: FUNCTIONS ======================================================== #


def load_pipeline_config(
    path: Path | str,
    *,
    substitute: bool = False,
    env: Mapping[str, str] | None = None,
) -> PipelineConfig:
    """
    Read a pipeline YAML file into a PipelineConfig dataclass.

    Delegates to PipelineConfig.from_yaml for the actual construction and
    optional variable substitution.
    """

    return PipelineConfig.from_yaml(path, substitute=substitute, env=env)


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class PipelineConfig:
    """
    Configuration for the data processing pipeline.
    """

    # -- Attributes -- #

    name: str | None = None
    version: str | None = None
    profile: ProfileConfig = field(default_factory=ProfileConfig)
    vars: dict[str, Any] = field(default_factory=dict)

    apis: dict[str, ApiConfig] = field(default_factory=dict)
    databases: dict[str, dict[str, Any]] = field(default_factory=dict)
    file_systems: dict[str, dict[str, Any]] = field(default_factory=dict)

    sources: list[Source] = field(default_factory=list)
    validations: dict[str, dict[str, Any]] = field(default_factory=dict)
    transforms: dict[str, dict[str, Any]] = field(default_factory=dict)
    targets: list[Target] = field(default_factory=list)
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
        Create a PipelineConfig instance from a YAML file.

        Parameters
        ----------
        path : Path | str
            The path to the YAML file.
        substitute : bool, optional
            Whether to perform variable substitution, by default False.
        env : Mapping[str, str] | None, optional
            An optional mapping of environment variables to use for
            substitution, by default None (uses os.environ).

        Returns
        -------
        Self
            The created PipelineConfig instance.
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
        Create a PipelineConfig instance from a dictionary.

        Parameters
        ----------
        raw : Mapping[str, Any]
            The raw configuration dictionary.

        Returns
        -------
        Self
            The created PipelineConfig instance.
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
