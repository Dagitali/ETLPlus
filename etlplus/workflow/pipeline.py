"""
:mod:`etlplus.workflow.pipeline` module.

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
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any
from typing import Self

from ..api import ApiConfig
from ..connector import Connector
from ..connector import parse_connector
from ..file import File
from ..file import FileFormat
from ..types import StrAnyMap
from ..utils import coerce_dict
from ..utils import deep_substitute
from ..utils import maybe_mapping
from .jobs import JobConfig
from .profile import ProfileConfig

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'PipelineConfig',
    # Functions
    'load_pipeline_config',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _build_connectors(
    raw: StrAnyMap,
    *,
    key: str,
) -> list[Connector]:
    """
    Parse connector entries from a list under ``raw[key]``.

    Parameters
    ----------
    raw : StrAnyMap
        Raw pipeline mapping.
    key : str
        Key pointing to connector entries (e.g., ``"sources"``).

    Returns
    -------
    list[Connector]
        Parsed connector instances.
    """
    return list(
        _collect_parsed(raw.get(key, []) or [], _parse_connector_entry),
    )


def _collect_parsed[T](
    items: Any,
    parser: Callable[[Any], T | None],
) -> list[T]:
    """
    Collect parsed items from ``raw[key]`` using a tolerant parser.

    Parameters
    ----------
    items : Any
        List-like payload to parse.
    parser : Callable[[Any], T | None]
        Parser that returns an instance or ``None`` for invalid entries.

    Returns
    -------
    list[T]
        Parsed items, excluding invalid entries.
    """
    parsed_items: list[T] = []
    for entry in items or []:
        parsed = parser(entry)
        if parsed is not None:
            parsed_items.append(parsed)
    return parsed_items


def _parse_connector_entry(
    obj: Any,
) -> Connector | None:
    """
    Parse a connector mapping into a concrete connector instance.

    Parameters
    ----------
    obj : Any
        Candidate connector mapping.

    Returns
    -------
    Connector | None
        Parsed connector instance or ``None`` when invalid.
    """
    if not (entry := maybe_mapping(obj)):
        return None
    try:
        return parse_connector(entry)
    except TypeError:
        return None


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


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
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
    table_schemas : list[dict[str, Any]]
        Optional DDL-style table specifications used by the render command.
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
    table_schemas: list[dict[str, Any]] = field(default_factory=list)

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
        substitute : bool, optional
            Perform variable substitution after initial parse. Defaults to
            ``False``.
        env : Mapping[str, str] | None, optional
            Environment mapping used for substitution; if omitted use
            ``os.environ``. Defaults to ``None``.

        Returns
        -------
        Self
            Parsed pipeline configuration.

        Raises
        ------
        TypeError
            If the YAML root is not a mapping/object.
        """
        raw = File(Path(path), FileFormat.YAML).read()
        if not isinstance(raw, dict):
            raise TypeError('Pipeline YAML must have a mapping/object root')

        cfg = cls.from_dict(raw)

        if substitute:
            # Merge order: profile.env first (lowest), then provided env or
            # os.environ (highest). External env overrides profile defaults.
            base_env = dict(getattr(cfg.profile, 'env', {}) or {})
            external = dict(env) if env is not None else dict(os.environ)
            env_map = base_env | external
            resolved = deep_substitute(raw, cfg.vars, env_map)
            cfg = cls.from_dict(resolved)

        return cfg

    # -- Class Methods -- #

    @classmethod
    def from_dict(
        cls,
        raw: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into a ``PipelineConfig`` instance.

        Parameters
        ----------
        raw : StrAnyMap
            Raw pipeline mapping.

        Returns
        -------
        Self
            Parsed pipeline configuration.
        """
        # Basic metadata
        name = raw.get('name')
        version = raw.get('version')

        # Profile and vars
        prof_raw = maybe_mapping(raw.get('profile')) or {}
        profile = ProfileConfig.from_obj(prof_raw)
        vars_map: dict[str, Any] = coerce_dict(raw.get('vars'))

        # APIs
        apis: dict[str, ApiConfig] = {}
        api_block = maybe_mapping(raw.get('apis')) or {}
        for api_name, api_obj in api_block.items():
            apis[str(api_name)] = ApiConfig.from_obj(api_obj)

        # Databases and file systems (pass-through structures)
        databases = coerce_dict(raw.get('databases'))
        file_systems = coerce_dict(raw.get('file_systems'))

        # Sources
        sources = _build_connectors(raw, key='sources')

        # Validations/Transforms
        validations = coerce_dict(raw.get('validations'))
        transforms = coerce_dict(raw.get('transforms'))

        # Targets
        targets = _build_connectors(raw, key='targets')

        # Jobs
        jobs: list[JobConfig] = _collect_parsed(
            raw.get('jobs', []) or [],
            JobConfig.from_obj,
        )

        # Table schemas (optional, tolerant pass-through structures).
        table_schemas: list[dict[str, Any]] = []
        for entry in raw.get('table_schemas', []) or []:
            spec = maybe_mapping(entry)
            if spec is not None:
                table_schemas.append(dict(spec))

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
            table_schemas=table_schemas,
        )
