"""
:mod:`etlplus.workflow.jobs` module.

Data classes modeling job orchestration references (extract, validate,
transform, load).

Notes
-----
- Lightweight references used inside :class:`PipelineConfig` to avoid storing
    large nested structures.
- All attributes are simple and optional where appropriate, keeping parsing
    tolerant.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Self

from ..types import StrAnyMap
from ..utils import coerce_dict
from ..utils import maybe_mapping

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'ExtractRef',
    'JobConfig',
    'LoadRef',
    'TransformRef',
    'ValidationRef',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_optional_str(value: Any) -> str | None:
    """
    Normalize optional string values, coercing non-strings when needed.

    Parameters
    ----------
    value : Any
        Optional value to normalize.

    Returns
    -------
    str | None
        ``None`` when *value* is ``None``; otherwise a string value.
    """
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)


def _parse_depends_on(
    value: Any,
) -> list[str]:
    """
    Normalize dependency declarations into a string list.

    Parameters
    ----------
    value : Any
        Input dependency specification (string or list of strings).

    Returns
    -------
    list[str]
        Normalized dependency list.
    """
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return [entry for entry in value if isinstance(entry, str)]
    return []


def _require_str(
    data: StrAnyMap,
    key: str,
) -> str | None:
    """
    Extract a required string field from a mapping.

    Parameters
    ----------
    data : StrAnyMap
        Mapping containing the target field.
    key : str
        Field name to extract.

    Returns
    -------
    str | None
        The string value when present and valid; otherwise ``None``.
    """
    value = data.get(key)
    return value if isinstance(value, str) else None


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
        data = maybe_mapping(obj)
        if not data:
            return None
        if (source := _require_str(data, 'source')) is None:
            return None
        return cls(source=source, options=coerce_dict(data.get('options')))


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
        data = maybe_mapping(obj)
        if not data:
            return None
        if (name := _require_str(data, 'name')) is None:
            return None

        return cls(
            name=name,
            description=_coerce_optional_str(data.get('description')),
            depends_on=_parse_depends_on(data.get('depends_on')),
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
        data = maybe_mapping(obj)
        if not data:
            return None
        if (target := _require_str(data, 'target')) is None:
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
        data = maybe_mapping(obj)
        if not data:
            return None
        if (pipeline := _require_str(data, 'pipeline')) is None:
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
        data = maybe_mapping(obj)
        if not data:
            return None
        if (ruleset := _require_str(data, 'ruleset')) is None:
            return None
        return cls(
            ruleset=ruleset,
            severity=_coerce_optional_str(data.get('severity')),
            phase=_coerce_optional_str(data.get('phase')),
        )
