"""
etlplus.validation.utils module.

A module providing helpers for data validation.
"""
from __future__ import annotations

from typing import Any
from typing import Callable


# SECTION: FUNCTIONS ======================================================== #


def maybe_validate(
    payload: Any,
    when: str,
    *,
    enabled: bool,
    rules: dict[str, Any],
    phase: str,
    severity: str,
    validate_fn: Callable[[Any, dict[str, Any]], Any],
    print_json_fn: Callable[[Any], Any],
) -> Any:
    """
    Run validation conditionally by phase and severity.

    Returns the (possibly modified) payload when validation passes, or raises
    ValueError when validation fails and severity=="error".

    Parameters
    ----------
    payload : Any
        The data payload to validate.
    when : str
        When to run validation: "before_transform", "after_transform", or
        "both".
    enabled : bool
        Whether validation is enabled.
    rules : dict[str, Any]
        Validation rules to apply.
    phase : str
        Current phase: "before_transform" or "after_transform".
    severity : str
        Severity level: "error" or "warn".
    validate_fn : Callable[[Any, dict[str, Any]], Any]
        Function to run validation, taking payload and rules.
    print_json_fn : Callable[[Any], Any]
        Function to print JSON messages.

    Returns
    -------
    Any
        The (possibly modified) payload when validation passes, or raises
        ValueError when validation fails and severity=="error".
    """
    if not enabled:
        return payload

    phase = (phase or 'before_transform').lower()
    when = (when or '').lower()

    active = (
        (
            when == 'before_transform'
            and phase in {'before_transform', 'both'}
        )
        or (
            when == 'after_transform'
            and phase in {'after_transform', 'both'}
        )
    )
    if not active:
        return payload

    res = validate_fn(payload, rules)
    if res.get('valid', False):
        res_data = res.get('data')
        return res_data if res_data is not None else payload

    msg = {'status': 'validation_failed', 'result': res}
    print_json_fn(msg)
    if (severity or 'error').lower() == 'warn':
        return payload

    raise ValueError('Validation failed')
