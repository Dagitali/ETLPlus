"""
:mod:`etlplus.ops.utils` module.

Utility helpers for conditional data ops orchestration.

The helpers defined here embrace a "high cohesion, low coupling" design by
isolating normalization, configuration, and logging responsibilities. The
resulting surface keeps ``maybe_validate`` focused on orchestration while
offloading ancillary concerns to composable helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any
from typing import Literal
from typing import Self
from typing import TypedDict
from typing import cast

from ..types import StrAnyMap
from ..utils import normalize_choice

# SECTION: TYPED DICTIONARIES =============================================== #


class ValidationResult(TypedDict, total=False):
    """Shape returned by ``validate_fn`` callables."""

    valid: bool
    data: Any
    errors: Any
    field_errors: Any


# SECTION: TYPE ALIASES ===================================================== #


type Ruleset = StrAnyMap

type ValidationPhase = Literal['before_transform', 'after_transform']
type ValidationWindow = Literal['before_transform', 'after_transform', 'both']
type ValidationSeverity = Literal['warn', 'error']

type ValidateFn = Callable[[Any, Ruleset], ValidationResult]
type PrintFn = Callable[[Any], None]


# SECTION: INTERNAL CONSTANTS ============================================== #


_PHASE_CHOICES = MappingProxyType(
    {
        'before_transform': 'before_transform',
        'after_transform': 'after_transform',
    },
)
_SEVERITY_CHOICES = MappingProxyType(
    {
        'warn': 'warn',
        'error': 'error',
    },
)
_WINDOW_CHOICES = MappingProxyType(
    {
        'before_transform': 'before_transform',
        'after_transform': 'after_transform',
        'both': 'both',
    },
)


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True, frozen=True)
class ValidationSettings:
    """
    Normalized validation configuration.

    Attributes
    ----------
    enabled : bool
        Global flag to toggle validation.
    rules : Ruleset | None
        Validation rules to apply. ``None`` or empty mappings short-circuit.
    phase : ValidationPhase
        Current pipeline phase requesting validation. Accepts
        ``"before_transform"`` or ``"after_transform"``.
    window : ValidationWindow
        Configured validation window. Accepts ``"before_transform"``,
        ``"after_transform"``, or ``"both"``.
    severity : ValidationSeverity
        Failure severity (``"warn"`` or ``"error"``).
    """

    # -- Attributes -- #

    enabled: bool
    rules: Ruleset | None
    phase: ValidationPhase
    window: ValidationWindow
    severity: ValidationSeverity

    # -- Class Methods -- #

    @classmethod
    def from_raw(
        cls,
        *,
        enabled: bool,
        rules: Ruleset | None,
        phase: str | None,
        window: str | None,
        severity: str | None,
    ) -> Self:
        """
        Construct a settings object from untrusted user configuration.

        Parameters
        ----------
        enabled : bool
            Global flag to toggle validation.
        rules : Ruleset | None
            Validation rules to apply. ``None`` or empty mappings
            short-circuit.
        phase : str | None
            Current pipeline phase requesting validation. Accepts
            ``"before_transform"`` or ``"after_transform"``.
        window : str | None
            Configured validation window. Accepts ``"before_transform"``,
            ``"after_transform"``, or ``"both"``.
        severity : str | None
            Failure severity (``"warn"`` or ``"error"``).

        Returns
        -------
        Self
            Normalized settings object.
        """
        return cls(
            enabled=bool(enabled),
            rules=rules if rules else None,
            phase=_normalize_phase(phase),
            window=_normalize_window(window),
            severity=_normalize_severity(severity),
        )

    # -- Instance Methods -- #

    def should_run(self) -> bool:
        """
        Return ``True`` when validation should execute for the phase.

        Returns
        -------
        bool
            ``True`` when validation should execute for the phase.
        """
        if not self.enabled or not self.rules:
            return False
        return _should_validate(self.window, self.phase)


# SECTION: FUNCTIONS ======================================================== #


def maybe_validate(
    payload: Any,
    when: str,
    *,
    enabled: bool,
    rules: Ruleset | None,
    phase: str,
    severity: str,
    validate_fn: ValidateFn,
    print_json_fn: PrintFn,
) -> Any:
    """
    Run validation based on declarative configuration.

    Parameters
    ----------
    payload : Any
        Arbitrary payload supplied by the pipeline stage.
    when : str
        Desired validation window. Accepts ``"before_transform"``,
        ``"after_transform"``, or ``"both"``.
    enabled : bool
        Global flag to toggle validation.
    rules : Ruleset | None
        Validation rules to apply. ``None`` or empty mappings short-circuit.
    phase : str
        Current pipeline phase requesting validation.
    severity : str
        Failure severity (``"warn"`` or ``"error"``).
    validate_fn : ValidateFn
        Engine that performs validation and returns a
        :class:`ValidationResult` instance.
    print_json_fn : PrintFn
        Structured logger invoked when validation fails.

    Returns
    -------
    Any
        *payload* when validation is skipped or when severity is ``"warn"``
        and the validation fails. Returns the validator ``data`` payload when
        validation succeeds.

    Raises
    ------
    ValueError
        Raised when validation fails and *severity* is ``"error"``.

    Examples
    --------
    >>> maybe_validate(
    ...     {'valid': True},
    ...     when='both',
    ...     enabled=True,
    ...     rules={'required': ['valid']},
    ...     phase='before_transform',
    ...     severity='warn',
    ...     validate_fn=lambda payload, rules: {
    ...         'valid': True,
    ...         'data': payload,
    ...     },
    ...     print_json_fn=lambda payload: payload,
    ... )
    {'valid': True}
    """
    settings = ValidationSettings.from_raw(
        enabled=enabled,
        rules=rules,
        phase=phase,
        window=when,
        severity=severity,
    )
    if not settings.should_run():
        return payload

    ruleset = settings.rules
    assert ruleset is not None  # Guarded by should_run()

    result = validate_fn(payload, ruleset)
    if result.get('valid', False):
        return result.get('data', payload)

    _log_failure(
        print_json_fn,
        phase=settings.phase,
        window=settings.window,
        ruleset_name=_rule_name(ruleset),
        result=result,
    )
    if settings.severity == 'warn':
        return payload

    raise ValueError('Validation failed')


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _log_failure(
    printer: PrintFn,
    *,
    phase: ValidationPhase,
    window: ValidationWindow,
    ruleset_name: str | None,
    result: ValidationResult,
) -> None:
    """
    Emit a structured message describing the failed validation.

    Parameters
    ----------
    printer : PrintFn
        Structured logger invoked when validation fails.
    phase : ValidationPhase
        Current pipeline phase requesting validation.
    window : ValidationWindow
        Configured validation window.
    ruleset_name : str | None
        Name of the validation ruleset.
    result : ValidationResult
        Result of the failed validation.
    """
    printer(
        {
            'status': 'validation_failed',
            'phase': phase,
            'when': window,
            'ruleset': ruleset_name,
            'result': result,
        },
    )


def _normalize_phase(
    value: str | None,
) -> ValidationPhase:
    """
    Normalize arbitrary text into a known validation phase.

    Parameters
    ----------
    value : str | None
        Untrusted text to normalize.

    Returns
    -------
    ValidationPhase
        Normalized validation phase. Defaults to ``"before_transform"`` when
        unspecified.
    """
    return cast(
        ValidationPhase,
        normalize_choice(
            value,
            mapping=_PHASE_CHOICES,
            default='before_transform',
        ),
    )


def _normalize_severity(
    value: str | None,
) -> ValidationSeverity:
    """
    Normalize severity, defaulting to ``"error"`` when unspecified.

    Parameters
    ----------
    value : str | None
        Untrusted text to normalize.

    Returns
    -------
    ValidationSeverity
        Normalized severity. Defaults to ``"error"`` when unspecified.
    """
    return cast(
        ValidationSeverity,
        normalize_choice(
            value,
            mapping=_SEVERITY_CHOICES,
            default='error',
        ),
    )


def _normalize_window(
    value: str | None,
) -> ValidationWindow:
    """
    Normalize the configured validation window.

    Parameters
    ----------
    value : str | None
        Untrusted text to normalize.

    Returns
    -------
    ValidationWindow
        Normalized validation window. Defaults to ``"both"`` when unspecified.
    """
    return cast(
        ValidationWindow,
        normalize_choice(
            value,
            mapping=_WINDOW_CHOICES,
            default='both',
        ),
    )


def _rule_name(
    rules: Ruleset,
) -> str | None:
    """
    Best-effort extraction of a ruleset identifier.

    Parameters
    ----------
    rules : Ruleset
        Untrusted ruleset configuration.

    Returns
    -------
    str | None
        Name of the ruleset when available. Returns ``None`` when the ruleset
        lacks a name or when the ruleset is not a mapping.
    """
    getter = getattr(rules, 'get', None)
    if callable(getter):
        return getter('name')
    return None


def _should_validate(
    window: ValidationWindow,
    phase: ValidationPhase,
) -> bool:
    """
    Return ``True`` when the validation window matches the phase.

    Parameters
    ----------
    window : ValidationWindow
        Configured validation window. Accepts ``"before_transform"``,
        ``"after_transform"``, or ``"both"``.
    phase : ValidationPhase
        Current pipeline phase requesting validation. Accepts
        ``"before_transform"`` or ``"after_transform"``.

    Returns
    -------
    bool
        ``True`` when the validation window matches the phase.
    """
    return window == 'both' or window == phase
