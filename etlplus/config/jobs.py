"""
:mod:`etlplus.config.jobs` module.

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
from typing import Self

from ..utils import coerce_dict
from ..utils import maybe_mapping

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ExtractRef',
    'JobConfig',
    'LoadRef',
    'TransformRef',
    'ValidationRef',
]


# SECTION: TYPE ALIASES ===================================================== #


# SECTION: CLASSES ========================================================== #


@dataclass(kw_only=True, slots=True)
class ExtractRef:
    """
    Reference to a data source for extraction.

    Attributes
    ----------
    source : str
        Name of the source connector.
    options : dict[str, Any]
        Optional extract-time options (e.g., query parameters overrides).
    """

    # -- Attributes -- #

    source: str
    options: dict[str, Any] = field(default_factory=dict)

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: Any,
    ) -> Self | None:
        """Parse a mapping into an :class:`ExtractRef` instance.

        Parameters
        ----------
        obj : Any
            Mapping with ``source`` and optional ``options``.

        Returns
        -------
        Self | None
            Parsed reference or ``None`` when the payload is invalid.
        """
        data = maybe_mapping(obj)
        if not data:
            return None
        source = data.get('source')
        if not isinstance(source, str):
            return None
        return cls(
            source=source,
            options=coerce_dict(data.get('options')),
        )


@dataclass(kw_only=True, slots=True)
class JobConfig:
    """
    Configuration for a data processing job.

    Attributes
    ----------
    name : str
        Unique job name.
    description : str | None
        Optional human-friendly description.
    extract : ExtractRef | None
        Extraction reference.
    validate : ValidationRef | None
        Validation reference.
    transform : TransformRef | None
        Transform reference.
    load : LoadRef | None
        Load reference.
    """

    # -- Attributes -- #

    name: str
    description: str | None = None
    extract: ExtractRef | None = None
    validate: ValidationRef | None = None
    transform: TransformRef | None = None
    load: LoadRef | None = None

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: Any,
    ) -> Self | None:
        """Parse a mapping into a :class:`JobConfig` instance.

        Parameters
        ----------
        obj : Any
            Mapping describing a job block.

        Returns
        -------
        Self | None
            Parsed job configuration or ``None`` if invalid.
        """
        data = maybe_mapping(obj)
        if not data:
            return None
        name = data.get('name')
        if not isinstance(name, str):
            return None

        description = data.get('description')
        if description is not None and not isinstance(description, str):
            description = str(description)

        return cls(
            name=name,
            description=description,
            extract=ExtractRef.from_obj(data.get('extract')),
            validate=ValidationRef.from_obj(data.get('validate')),
            transform=TransformRef.from_obj(data.get('transform')),
            load=LoadRef.from_obj(data.get('load')),
        )


@dataclass(kw_only=True, slots=True)
class LoadRef:
    """
    Reference to a data target for loading.

    Attributes
    ----------
    target : str
        Name of the target connector.
    overrides : dict[str, Any]
        Optional load-time overrides (e.g., headers).
    """

    # -- Attributes -- #

    target: str
    overrides: dict[str, Any] = field(default_factory=dict)

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: Any,
    ) -> Self | None:
        """Parse a mapping into a :class:`LoadRef` instance.

        Parameters
        ----------
        obj : Any
            Mapping with ``target`` and optional ``overrides``.

        Returns
        -------
        Self | None
            Parsed reference or ``None`` when invalid.
        """
        data = maybe_mapping(obj)
        if not data:
            return None
        target = data.get('target')
        if not isinstance(target, str):
            return None
        return cls(
            target=target,
            overrides=coerce_dict(data.get('overrides')),
        )


@dataclass(kw_only=True, slots=True)
class TransformRef:
    """
    Reference to a transformation pipeline.

    Attributes
    ----------
    pipeline : str
        Name of the transformation pipeline.
    """

    # -- Attributes -- #

    pipeline: str

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: Any,
    ) -> Self | None:
        """Parse a mapping into a :class:`TransformRef` instance.

        Parameters
        ----------
        obj : Any
            Mapping with ``pipeline``.

        Returns
        -------
        Self | None
            Parsed reference or ``None`` when invalid.
        """
        data = maybe_mapping(obj)
        if not data:
            return None
        pipeline = data.get('pipeline')
        if not isinstance(pipeline, str):
            return None
        return cls(pipeline=pipeline)


@dataclass(kw_only=True, slots=True)
class ValidationRef:
    """
    Reference to a validation rule set.

    Attributes
    ----------
    ruleset : str
        Name of the validation rule set.
    severity : str | None
        Severity level (``"warn"`` or ``"error"``).
    phase : str | None
        Execution phase (``"before_transform"``, ``"after_transform"``,
        or ``"both"``).
    """

    # -- Attributes -- #

    ruleset: str
    severity: str | None = None  # warn|error
    phase: str | None = None  # before_transform|after_transform|both

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: Any,
    ) -> Self | None:
        """Parse a mapping into a :class:`ValidationRef` instance.

        Parameters
        ----------
        obj : Any
            Mapping with ``ruleset`` plus optional metadata.

        Returns
        -------
        Self | None
            Parsed reference or ``None`` when invalid.
        """
        data = maybe_mapping(obj)
        if not data:
            return None
        ruleset = data.get('ruleset')
        if not isinstance(ruleset, str):
            return None
        severity = data.get('severity')
        if severity is not None and not isinstance(severity, str):
            severity = str(severity)
        phase = data.get('phase')
        if phase is not None and not isinstance(phase, str):
            phase = str(phase)
        return cls(
            ruleset=ruleset,
            severity=severity,
            phase=phase,
        )
