"""
:mod:`tests.unit.ops.test_u_ops_validation` module.

Unit tests for :mod:`etlplus.ops._validation`.
"""

from __future__ import annotations

from typing import Any
from typing import cast

import pytest

import etlplus.ops._validation as validation_mod
from etlplus.ops._validation import ValidationResultDict
from etlplus.ops._validation import ValidationSettings
from etlplus.ops._validation import maybe_validate

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


def _printer(messages: list[dict[str, object]]):
    """Build a simple structured logger for test assertions."""

    def _inner(message: dict[str, object]) -> None:
        messages.append(message)

    return _inner


def _successful_validator(
    calls: dict[str, int],
) -> validation_mod.ValidateFn:
    """Build a validator stub that returns the original payload."""

    def _validator(payload: Any, _rules: Any) -> ValidationResultDict:
        calls['count'] += 1
        return ValidationResultDict(valid=True, data=payload)

    return _validator


# SECTION: FIXTURES ========================================================= #


@pytest.fixture
def printer_calls() -> list[dict[str, object]]:
    """Collect structured log messages emitted by validation helpers."""
    return []


# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    ('helper_name', 'value', 'expected'),
    [
        ('_normalize_phase', None, 'before_transform'),
        ('_normalize_phase', 'after_transform', 'after_transform'),
        ('_normalize_phase', 'unknown', 'before_transform'),
        ('_normalize_window', None, 'both'),
        ('_normalize_window', 'after_transform', 'after_transform'),
        ('_normalize_window', 'unknown', 'both'),
        ('_normalize_severity', None, 'error'),
        ('_normalize_severity', 'warn', 'warn'),
        ('_normalize_severity', 'unknown', 'error'),
    ],
)
def test_normalize_helpers(
    helper_name: str,
    value: str | None,
    expected: str,
) -> None:
    """Normalization helpers should coerce invalid values to defaults."""
    helper = getattr(validation_mod, helper_name)
    assert helper(value) == expected


@pytest.mark.parametrize(
    ('enabled', 'rules', 'phase', 'window', 'severity', 'expected'),
    [
        (False, {'required': []}, 'before_transform', 'both', 'error', False),
        (True, None, 'before_transform', 'both', 'error', False),
        (True, {}, 'before_transform', 'both', 'warn', False),
        (
            True,
            {'required': []},
            'before_transform',
            'before_transform',
            'error',
            True,
        ),
        (
            True,
            {'required': []},
            'after_transform',
            'before_transform',
            'warn',
            False,
        ),
    ],
)
def test_validation_settings_should_run(
    enabled: bool,
    rules: dict[str, object] | None,
    phase: str | None,
    window: str | None,
    severity: str | None,
    expected: bool,
) -> None:
    """Validation settings should normalize raw config consistently."""
    settings = ValidationSettings.from_raw(
        enabled=enabled,
        rules=rules,
        phase=phase,
        window=window,
        severity=severity,
    )
    assert settings.should_run() is expected


@pytest.mark.parametrize(
    ('window', 'phase', 'expected'),
    [
        ('both', 'before_transform', True),
        ('both', 'after_transform', True),
        ('before_transform', 'before_transform', True),
        ('after_transform', 'after_transform', True),
        ('before_transform', 'after_transform', False),
        ('after_transform', 'before_transform', False),
    ],
)
def test_should_validate_matrix(
    window: validation_mod.ValidationWindow,
    phase: validation_mod.ValidationPhase,
    expected: bool,
) -> None:
    """Window matching should only run for compatible phases."""
    assert validation_mod._should_validate(window, phase) is expected


@pytest.mark.parametrize(
    ('rules', 'expected'),
    [
        ({'name': 'customer_rules'}, 'customer_rules'),
        ({}, None),
        (cast(Any, object()), None),
    ],
)
def test_rule_name_best_effort(
    rules: Any,
    expected: str | None,
) -> None:
    """Rule-name extraction should degrade cleanly for non-mappings."""
    assert validation_mod._rule_name(rules) == expected


def test_log_failure_emits_structured_payload(
    printer_calls: list[dict[str, object]],
) -> None:
    """Failure logging should emit a stable structured payload."""
    result = ValidationResultDict(valid=False, errors=['boom'])

    validation_mod._log_failure(
        _printer(printer_calls),
        phase='after_transform',
        window='both',
        ruleset_name='ruleset',
        result=result,
    )

    assert printer_calls == [
        {
            'status': 'validation_failed',
            'phase': 'after_transform',
            'when': 'both',
            'ruleset': 'ruleset',
            'result': result,
        },
    ]


@pytest.mark.parametrize(
    ('enabled', 'rules'),
    [
        (False, {'required': []}),
        (True, None),
        (True, {}),
    ],
)
def test_maybe_validate_short_circuits(
    enabled: bool,
    rules: dict[str, object] | None,
) -> None:
    """Disabled or rule-less validation should return the original payload."""
    calls = {'count': 0}
    payload = {'ok': True}

    result = maybe_validate(
        payload,
        when='before_transform',
        enabled=enabled,
        rules=rules,
        phase='before_transform',
        severity='error',
        validate_fn=_successful_validator(calls),
        print_json_fn=lambda _: None,
    )

    assert result is payload
    assert calls['count'] == 0


@pytest.mark.parametrize(
    ('when', 'phase'),
    [
        ('both', 'after_transform'),
        ('before_transform', 'before_transform'),
    ],
)
def test_maybe_validate_runs_for_matching_window(
    when: str,
    phase: str,
) -> None:
    """Matching windows should execute the validator exactly once."""
    calls = {'count': 0}
    payload = {'ok': True}

    result = maybe_validate(
        payload,
        when=when,
        enabled=True,
        rules={'required': []},
        phase=phase,
        severity='error',
        validate_fn=_successful_validator(calls),
        print_json_fn=lambda _: None,
    )

    assert result is payload
    assert calls['count'] == 1


def test_success_returns_result_data() -> None:
    """Successful validation should return the validator-provided payload."""

    def validator(_payload: Any, _rules: Any) -> ValidationResultDict:
        return ValidationResultDict(valid=True, data={'mutated': True})

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


def test_warn_severity_logs_without_raising(
    printer_calls: list[dict[str, object]],
) -> None:
    """Warn severity should log and preserve the original payload."""

    def validator(_payload: Any, _rules: Any) -> ValidationResultDict:
        return ValidationResultDict(valid=False, errors=['boom'])

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
    assert printer_calls


def test_error_severity_raises_value_error(
    printer_calls: list[dict[str, object]],
) -> None:
    """Error severity should raise after emitting a failure log."""

    def validator(_payload: Any, _rules: Any) -> ValidationResultDict:
        return ValidationResultDict(valid=False, errors=['boom'])

    with pytest.raises(ValueError, match='Validation failed'):
        maybe_validate(
            {'ok': True},
            when='after_transform',
            enabled=True,
            rules={'required': []},
            phase='after_transform',
            severity='error',
            validate_fn=validator,
            print_json_fn=_printer(printer_calls),
        )

    assert printer_calls
