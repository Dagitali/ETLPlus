"""
etlplus.config.jobs
===================

Data classes modeling job orchestration references (extract, validate,
transform, load).

Notes
-----
- Lightweight references used inside ``PipelineConfig`` to avoid storing
    large nested structures.
- All attributes are simple and optional where appropriate, keeping parsing
    tolerant.
"""
from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any


# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ExtractRef',
    'JobConfig',
    'LoadRef',
    'TransformRef',
    'ValidationRef',
]


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class ExtractRef:
    """
    Reference to a data source for extraction.
    """

    # -- Attributes -- #

    source: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JobConfig:
    """
    Configuration for a data processing job.
    """

    # -- Attributes -- #

    name: str
    description: str | None = None
    extract: ExtractRef | None = None
    validate: ValidationRef | None = None
    transform: TransformRef | None = None
    load: LoadRef | None = None


@dataclass(slots=True)
class LoadRef:
    """
    Reference to a data target for loading.
    """

    # -- Attributes -- #

    target: str
    overrides: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TransformRef:
    """
    Reference to a transformation pipeline.
    """

    # -- Attributes -- #

    pipeline: str


@dataclass(slots=True)
class ValidationRef:
    """
    Reference to a validation rule set.
    """

    # -- Attributes -- #

    ruleset: str
    severity: str | None = None  # warn|error
    phase: str | None = None     # before_transform|after_transform|both
