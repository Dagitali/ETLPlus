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


@pytest.fixture(name='printer_calls')
def printer_calls_fixture() -> list[dict[str, object]]:
    """Collect structured log messages emitted by validation helpers."""
    return []


# SECTION: TESTS ============================================================ #


class TestNormalizationHelpers:
    """Unit tests for validation normalization helpers."""

    @pytest.mark.parametrize(
        ('helper_name', 'value', 'expected'),
        [
            pytest.param('_normalize_phase', None, 'before_transform', id='phase-none'),
            pytest.param(
                '_normalize_phase',
                'after_transform',
                'after_transform',
                id='phase-valid',
            ),
            pytest.param(
                '_normalize_phase', 'unknown', 'before_transform', id='phase-invalid',
            ),
            pytest.param('_normalize_window', None, 'both', id='window-none'),
            pytest.param(
                '_normalize_window',
                'after_transform',
                'after_transform',
                id='window-valid',
            ),
            pytest.param('_normalize_window', 'unknown', 'both', id='window-invalid'),
            pytest.param('_normalize_severity', None, 'error', id='severity-none'),
            pytest.param('_normalize_severity', 'warn', 'warn', id='severity-valid'),
            pytest.param(
                '_normalize_severity', 'unknown', 'error', id='severity-invalid',
            ),
        ],
    )
    def test_normalize_helpers(
        self,
        helper_name: str,
        value: str | None,
        expected: str,
    ) -> None:
        """
        Test that normalization helpers coerce invalid values to defaults.
        """
        assert getattr(validation_mod, helper_name)(value) == expected

    @pytest.mark.parametrize(
        ('rules', 'expected'),
        [
            pytest.param(
                {'name': 'customer_rules'}, 'customer_rules', id='named-ruleset',
            ),
            pytest.param({}, None, id='missing-name'),
            pytest.param(cast(Any, object()), None, id='non-mapping'),
        ],
    )
    def test_rule_name_best_effort(
        self,
        rules: Any,
        expected: str | None,
    ) -> None:
        """Test that rule-name extraction degrades cleanly for non-mappings."""
        assert validation_mod._rule_name(rules) == expected

    def test_log_failure_emits_structured_payload(
        self,
        printer_calls: list[dict[str, object]],
    ) -> None:
        """Test that failure logging emits a stable structured payload."""
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


class TestValidationSettings:
    """Unit tests for `ValidationSettings` and window matching."""

    @pytest.mark.parametrize(
        ('enabled', 'rules', 'phase', 'window', 'severity', 'expected'),
        [
            pytest.param(
                False,
                {'required': []},
                'before_transform',
                'both',
                'error',
                False,
                id='disabled',
            ),
            pytest.param(
                True,
                None,
                'before_transform',
                'both',
                'error',
                False,
                id='missing-rules',
            ),
            pytest.param(
                True, {}, 'before_transform', 'both', 'warn', False, id='empty-rules',
            ),
            pytest.param(
                True,
                {'required': []},
                'before_transform',
                'before_transform',
                'error',
                True,
                id='matching-window',
            ),
            pytest.param(
                True,
                {'required': []},
                'after_transform',
                'before_transform',
                'warn',
                False,
                id='mismatched-window',
            ),
        ],
    )
    def test_validation_settings_should_run(
        self,
        enabled: bool,
        rules: dict[str, object] | None,
        phase: str | None,
        window: str | None,
        severity: str | None,
        expected: bool,
    ) -> None:
        """Test that validation settings  normalize raw config consistently."""
        assert (
            ValidationSettings.from_raw(
                enabled=enabled,
                rules=rules,
                phase=phase,
                window=window,
                severity=severity,
            ).should_run()
            is expected
        )

    @pytest.mark.parametrize(
        ('window', 'phase', 'expected'),
        [
            pytest.param('both', 'before_transform', True, id='both-before'),
            pytest.param('both', 'after_transform', True, id='both-after'),
            pytest.param(
                'before_transform', 'before_transform', True, id='before-before',
            ),
            pytest.param('after_transform', 'after_transform', True, id='after-after'),
            pytest.param(
                'before_transform', 'after_transform', False, id='before-after',
            ),
            pytest.param(
                'after_transform', 'before_transform', False, id='after-before',
            ),
        ],
    )
    def test_should_validate_matrix(
        self,
        window: validation_mod.ValidationWindow,
        phase: validation_mod.ValidationPhase,
        expected: bool,
    ) -> None:
        """Test that window matching only runs for compatible phases."""
        assert validation_mod._should_validate(window, phase) is expected


class TestMaybeValidate:
    """Unit tests for `maybe_validate()` orchestration."""

    @pytest.mark.parametrize(
        ('enabled', 'rules'),
        [
            pytest.param(False, {'required': []}, id='disabled'),
            pytest.param(True, None, id='missing-rules'),
            pytest.param(True, {}, id='empty-rules'),
        ],
    )
    def test_maybe_validate_short_circuits(
        self,
        enabled: bool,
        rules: dict[str, object] | None,
    ) -> None:
        """
        Test that disabled or rule-less validation returns the original
        payload.
        """
        calls = {'count': 0}
        payload = {'ok': True}

        assert (
            maybe_validate(
                payload,
                when='before_transform',
                enabled=enabled,
                rules=rules,
                phase='before_transform',
                severity='error',
                validate_fn=_successful_validator(calls),
                print_json_fn=lambda _: None,
            )
            is payload
        )
        assert calls['count'] == 0

    @pytest.mark.parametrize(
        ('when', 'phase'),
        [
            pytest.param('both', 'after_transform', id='both-after'),
            pytest.param('before_transform', 'before_transform', id='before-before'),
        ],
    )
    def test_maybe_validate_runs_for_matching_window(
        self,
        when: str,
        phase: str,
    ) -> None:
        """Test that matching validation windows execute the validator once."""
        calls = {'count': 0}
        payload = {'ok': True}

        assert (
            maybe_validate(
                payload,
                when=when,
                enabled=True,
                rules={'required': []},
                phase=phase,
                severity='error',
                validate_fn=_successful_validator(calls),
                print_json_fn=lambda _: None,
            )
            is payload
        )
        assert calls['count'] == 1

    def test_success_returns_result_data(self) -> None:
        """
        Test that successful validation returns the validator-provided payload.
        """

        def validator(_payload: Any, _rules: Any) -> ValidationResultDict:
            return ValidationResultDict(valid=True, data={'mutated': True})

        assert maybe_validate(
            {'ok': True},
            when='before_transform',
            enabled=True,
            rules={'required': []},
            phase='before_transform',
            severity='error',
            validate_fn=validator,
            print_json_fn=lambda _: None,
        ) == {'mutated': True}

    def test_warn_severity_logs_without_raising(
        self,
        printer_calls: list[dict[str, object]],
    ) -> None:
        """
        Test that warn severity logs without raising and preserves the original
        payload.
        """

        def validator(_payload: Any, _rules: Any) -> ValidationResultDict:
            return ValidationResultDict(valid=False, errors=['boom'])

        payload = {'ok': True}
        assert (
            maybe_validate(
                payload,
                when='after_transform',
                enabled=True,
                rules={'required': []},
                phase='after_transform',
                severity='warn',
                validate_fn=validator,
                print_json_fn=_printer(printer_calls),
            )
            is payload
        )
        assert printer_calls

    def test_error_severity_raises_value_error(
        self,
        printer_calls: list[dict[str, object]],
    ) -> None:
        """Test that error severity raises after emitting a failure log."""

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
