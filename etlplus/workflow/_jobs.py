"""
:mod:`etlplus.workflow._jobs` module.

Data classes modeling job orchestration references (extract, validate,
transform, load).

Notes
-----
- All attributes are simple and optional where appropriate, keeping parsing
    tolerant.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Self

from ..utils import FloatParser
from ..utils import IntParser
from ..utils import MappingFieldParser
from ..utils import MappingParser
from ..utils import SequenceParser
from ..utils import TextNormalizer
from ..utils import ValueParser

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'ExtractRef',
    'JobConfig',
    'JobRetryConfig',
    'LoadRef',
    'TransformRef',
    'ValidationRef',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_VALIDATION_PHASE_CHOICES = {
    'before_transform': 'before_transform',
    'after_transform': 'after_transform',
    'both': 'both',
}
_VALIDATION_SEVERITY_CHOICES = {
    'warn': 'warn',
    'error': 'error',
}


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _normalize_optional_choice(
    value: object,
    *,
    mapping: dict[str, str],
) -> str | None:
    """Return one optional canonical choice string when recognized."""
    text = ValueParser.optional_str(value)
    if text is None:
        return None
    return TextNormalizer.resolve_choice(text, mapping=mapping, default=text)


# SECTION: DATA CLASSES ===================================================== #


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
        """
        Parse a mapping into an :class:`ExtractRef` instance.

        Parameters
        ----------
        obj : Any
            Mapping with :attr:`source` and optional :attr:`options`.

        Returns
        -------
        Self | None
            Parsed reference or ``None`` when the payload is invalid.
        """
        data = MappingParser.optional(obj)
        if not data:
            return None
        if (source := MappingFieldParser.required_str(data, 'source')) is None:
            return None
        return cls(
            source=source,
            options=MappingParser.to_dict(data.get('options')),
        )


@dataclass(kw_only=True, slots=True, frozen=True)
class JobRetryConfig:
    """
    Optional retry policy for DAG-style job execution.

    Attributes
    ----------
    max_attempts : int
        Maximum number of attempts for the job, including the first attempt.
    backoff_seconds : float
        Fixed delay applied before each retry attempt.
    """

    # -- Attributes -- #

    max_attempts: int = 1
    backoff_seconds: float = 0.0

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: Any,
    ) -> Self | None:
        """
        Parse one retry policy mapping.

        Parameters
        ----------
        obj : Any
            Mapping containing retry controls.

        Returns
        -------
        Self | None
            Parsed retry policy or ``None`` when the payload is invalid.
        """
        data = MappingParser.optional(obj)
        if not data:
            return None
        return cls(
            max_attempts=IntParser.positive(data.get('max_attempts'), default=1),
            backoff_seconds=(
                FloatParser.parse(data.get('backoff_seconds'), default=0.0, minimum=0.0)
                or 0.0
            ),
        )

    # -- Instance Properties -- #

    @property
    def enabled(
        self,
    ) -> bool:
        """Return whether retries are enabled beyond the first attempt."""
        return self.max_attempts > 1


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
    depends_on : list[str]
        Optional job dependency list. Dependencies must refer to other jobs.
    extract : ExtractRef | None
        Extraction reference.
    validate : ValidationRef | None
        Validation reference.
    retry : JobRetryConfig | None
        Optional retry controls applied by DAG-style execution.
    transform : TransformRef | None
        Transform reference.
    load : LoadRef | None
        Load reference.
    """

    # -- Attributes -- #

    name: str
    description: str | None = None
    depends_on: list[str] = field(default_factory=list)
    extract: ExtractRef | None = None
    validate: ValidationRef | None = None
    retry: JobRetryConfig | None = None
    transform: TransformRef | None = None
    load: LoadRef | None = None

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: Any,
    ) -> Self | None:
        """
        Parse a mapping into a :class:`JobConfig` instance.

        Parameters
        ----------
        obj : Any
            Mapping describing a job block.

        Returns
        -------
        Self | None
            Parsed job configuration or ``None`` if invalid.
        """
        data = MappingParser.optional(obj)
        if not data:
            return None
        if (name := MappingFieldParser.required_str(data, 'name')) is None:
            return None

        return cls(
            name=name,
            description=ValueParser.optional_str(data.get('description')),
            depends_on=SequenceParser.str_list(data.get('depends_on')),
            extract=ExtractRef.from_obj(data.get('extract')),
            validate=ValidationRef.from_obj(data.get('validate')),
            retry=JobRetryConfig.from_obj(data.get('retry')),
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
        """
        Parse a mapping into a :class:`LoadRef` instance.

        Parameters
        ----------
        obj : Any
            Mapping with :attr:`target` and optional :attr:`overrides`.

        Returns
        -------
        Self | None
            Parsed reference or ``None`` when invalid.
        """
        data = MappingParser.optional(obj)
        if not data:
            return None
        if (target := MappingFieldParser.required_str(data, 'target')) is None:
            return None
        return cls(
            target=target,
            overrides=MappingParser.to_dict(data.get('overrides')),
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
        """
        Parse a mapping into a :class:`TransformRef` instance.

        Parameters
        ----------
        obj : Any
            Mapping with :attr:`pipeline`.

        Returns
        -------
        Self | None
            Parsed reference or ``None`` when invalid.
        """
        data = MappingParser.optional(obj)
        if not data:
            return None
        if (pipeline := MappingFieldParser.required_str(data, 'pipeline')) is None:
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
        """
        Parse a mapping into a :class:`ValidationRef` instance.

        Parameters
        ----------
        obj : Any
            Mapping with :attr:`ruleset` plus optional metadata.

        Returns
        -------
        Self | None
            Parsed reference or ``None`` when invalid.
        """
        data = MappingParser.optional(obj)
        if not data:
            return None
        if (ruleset := MappingFieldParser.required_str(data, 'ruleset')) is None:
            return None
        return cls(
            ruleset=ruleset,
            severity=_normalize_optional_choice(
                data.get('severity'),
                mapping=_VALIDATION_SEVERITY_CHOICES,
            ),
            phase=_normalize_optional_choice(
                data.get('phase'),
                mapping=_VALIDATION_PHASE_CHOICES,
            ),
        )
