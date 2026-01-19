"""
:mod:`tests.unit.ops.test_u_ops_utils` module.

Unit tests for :mod:`etlplus.ops.utils`.

Notes
-----
- Exercises both flat and profiled API shapes.
- Uses factories for building profile defaults mappings.
- Verifies precedence and propagation of headers and ``base_path``.
"""

from __future__ import annotations

import pytest

from etlplus.ops.utils import ValidationResult
from etlplus.ops.utils import maybe_validate

# SECTION: HELPERS ========================================================== #


def _printer(messages: list[dict[str, object]]):
    def _inner(message: dict[str, object]) -> None:
        messages.append(message)

    return _inner


# SECTION: TESTS ============================================================ #


def test_both_window_runs_for_after_phase() -> None:
    """
    Test that validation runs when phase is ``after_transform and when is both.
    """
    calls = {'count': 0}

    def validator(payload, _rules) -> ValidationResult:
        calls['count'] += 1
        return ValidationResult(valid=True, data=payload)

    payload = {'ok': True}
    result = maybe_validate(
        payload,
        when='both',
        enabled=True,
        rules={'required': []},
        phase='after_transform',
        severity='error',
        validate_fn=validator,
        print_json_fn=lambda _: None,
    )

    assert result is payload
    assert calls['count'] == 1


def test_error_severity_raises_value_error() -> None:
    """
    Test that validation raises a :class:`ValueError` when the severity is set
    to ``error``.
    """
    printer_calls: list[dict[str, object]] = []

    def validator(_payload, _rules) -> ValidationResult:
        return ValidationResult(valid=False, errors=['boom'])

    payload = {'ok': True}
    with pytest.raises(ValueError):
        maybe_validate(
            payload,
            when='after_transform',
            enabled=True,
            rules={'required': []},
            phase='after_transform',
            severity='error',
            validate_fn=validator,
            print_json_fn=_printer(printer_calls),
        )

    assert printer_calls, 'expected a log entry when validation fails'


def test_skip_when_disabled() -> None:
    """
    Test that validation is skipped when the validation is disabled.
    """
    calls = {'count': 0}

    def validator(payload, _rules) -> ValidationResult:
        calls['count'] += 1
        return ValidationResult(valid=True, data=payload)

    payload = {'ok': True}
    result = maybe_validate(
        payload,
        when='before_transform',
        enabled=False,
        rules={'required': []},
        phase='before_transform',
        severity='error',
        validate_fn=validator,
        print_json_fn=lambda _: None,
    )

    assert result is payload
    assert calls['count'] == 0


def test_skip_when_rules_missing() -> None:
    """
    Test that validation is skipped when the rules are missing.
    """
    calls = {'count': 0}

    def validator(payload, _rules) -> ValidationResult:
        calls['count'] += 1
        return ValidationResult(valid=True, data=payload)

    payload = {'ok': True}
    result = maybe_validate(
        payload,
        when='before_transform',
        enabled=True,
        rules={},
        phase='before_transform',
        severity='error',
        validate_fn=validator,
        print_json_fn=lambda _: None,
    )

    assert result is payload
    assert calls['count'] == 0


def test_success_returns_result_data() -> None:
    """
    Test that validation returns the mutated data when validation succeeds.
    """

    def validator(_payload, _rules) -> ValidationResult:
        return ValidationResult(valid=True, data={'mutated': True})

    payload = {'ok': True}
    result = maybe_validate(
        payload,
        when='before_transform',
        enabled=True,
        rules={'required': []},
        phase='before_transform',
        severity='error',
        validate_fn=validator,
        print_json_fn=lambda _: None,
    )

    assert result == {'mutated': True}


def test_warn_severity_logs_without_raising() -> None:
    """
    Test that validation logs a warning without raising an exception when the
    severity is set to ``warn``.
    """
    printer_calls: list[dict[str, object]] = []

    def validator(_payload, _rules) -> ValidationResult:
        return ValidationResult(valid=False, errors=['boom'])

    payload = {'ok': True}
    result = maybe_validate(
        payload,
        when='after_transform',
        enabled=True,
        rules={'required': []},
        phase='after_transform',
        severity='warn',
        validate_fn=validator,
        print_json_fn=_printer(printer_calls),
    )

    assert result is payload
    assert printer_calls, 'expected a log entry when validation fails'
