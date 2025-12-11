"""
:mod:`etlplus.validation.utils` module.

Validation helpers used by pipeline orchestration.
"""
from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from typing import Any
from typing import Literal
from typing import TypedDict

# SECTION: TYPED DICTIONARIES =============================================== #


class ValidationResult(TypedDict, total=False):
    """Shape returned by ``validate_fn`` callables."""

    valid: bool
    data: Any
    errors: Any
    field_errors: Any


# SECTION: TYPE ALIASES ===================================================== #


ValidationPhase = Literal['before_transform', 'after_transform']
ValidationWindow = Literal['before_transform', 'after_transform', 'both']
ValidationSeverity = Literal['warn', 'error']

ValidateFn = Callable[[Any, Mapping[str, Any]], ValidationResult]
PrintFn = Callable[[Any], None]


# SECTION: FUNCTIONS ======================================================== #


def maybe_validate(
    payload: Any,
    when: str,
    *,
    enabled: bool,
    rules: Mapping[str, Any] | None,
    phase: str,
    severity: str,
    validate_fn: ValidateFn,
    print_json_fn: PrintFn,
) -> Any:
    """
    Run validation conditionally by phase and severity.

    Parameters
    ----------
    payload : Any
        The data payload to validate.
    when : str
        When to run validation: ``"before_transform"``, ``"after_transform"``,
        or ``"both"``.
    enabled : bool
        Whether validation is enabled.
    rules : Mapping[str, Any] | None
        Validation rules to apply (``None`` or empty mappings short-circuit).
    phase : str
        Current phase: ``"before_transform"`` or ``"after_transform"``.
    severity : str
        Severity level: ``"error"`` or ``"warn"``.
    validate_fn : ValidateFn
        Function that runs validation and returns a ``ValidationResult``.
    print_json_fn : PrintFn
        Function that logs/prints structured JSON messages.

    Returns
    -------
    Any
        The (possibly modified) payload when validation passes. Raises
        ``ValueError`` on failure when severity is ``"error"``.

    Raises
    ------
    ValueError
        If validation fails and severity is "error".
    """
    if not enabled or not rules:
        return payload

    current_phase = _normalize_phase(phase)
    window = _normalize_window(when)
    severity_level = _normalize_severity(severity)

    if not _should_validate(window, current_phase):
        return payload

    result = validate_fn(payload, rules)
    if result.get('valid', False):
        return result.get('data', payload)

    print_json_fn(
        {
            'status': 'validation_failed',
            'phase': current_phase,
            'when': window,
            'ruleset': getattr(rules, 'get', lambda *_: None)('name'),
            'result': result,
        },
    )
    if severity_level == 'warn':
        return payload

    raise ValueError('Validation failed')


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _normalize_phase(value: str | None) -> ValidationPhase:
    """Normalize arbitrary text into a known validation phase."""

    match (value or '').strip().lower():
        case 'after_transform':
            return 'after_transform'
        case _:
            return 'before_transform'


def _normalize_severity(value: str | None) -> ValidationSeverity:
    """Normalize severity, defaulting to ``"error"`` when unspecified."""

    return 'warn' if (value or '').strip().lower() == 'warn' else 'error'


def _normalize_window(value: str | None) -> ValidationWindow:
    """Normalize the configured validation window."""

    match (value or '').strip().lower():
        case 'before_transform':
            return 'before_transform'
        case 'after_transform':
            return 'after_transform'
        case _:
            return 'both'


def _should_validate(
    window: ValidationWindow,
    phase: ValidationPhase,
) -> bool:
    """Return ``True`` when the validation window matches the phase."""

    return window == 'both' or window == phase
