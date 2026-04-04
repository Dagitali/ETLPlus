"""
:mod:`etlplus.ops.validate` module.

Validate dicts and lists of dicts using simple, schema-like rules.

This module provides a very small validation primitive that is intentionally
runtime-friendly (no heavy schema engines) and pairs with ETLPlus' JSON-like
types. It focuses on clear error messages and predictable behavior.

Highlights
----------
- Centralized type map and helpers for clarity and reuse.
- Consistent error wording; field and item paths like ``[2].email``.
- Small, focused public API with :func:`validate_field` and :func:`validate`.

Examples
--------
>>> rules = {
...     'name': {'required': True, 'type': 'string', 'minLength': 1},
...     'age': {'type': 'integer', 'min': 0},
... }
>>> data = {'name': 'Ada', 'age': 28}
>>> validate(data, rules)['valid']
True
"""

from __future__ import annotations

import re
from collections.abc import Callable
from collections.abc import Mapping
from typing import Any
from typing import Literal
from typing import TypedDict

from ..utils._types import JSONData
from ..utils._types import Record
from ..utils._types import StrAnyMap
from ._types import DataSourceArg
from .load import load_data as _load_data

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'FieldRulesDict',
    'FieldValidationDict',
    'ValidationDict',
    'validate_field',
    'validate',
]


# SECTION: TYPED DICTS ====================================================== #


class FieldRulesDict(TypedDict, total=False):
    """
    Validation rules for a single field.

    Keys are optional; absent keys imply no constraint.
    """

    required: bool
    type: Literal[
        'string',
        'number',
        'integer',
        'boolean',
        'array',
        'object',
    ]
    min: float
    max: float
    minLength: int
    maxLength: int
    pattern: str
    enum: list[Any]


class FieldValidationDict(TypedDict):
    """
    Validation result for a single field.

    Attributes
    ----------
    valid : bool
        Whether the field is valid.
    errors : list[str]
        List of error messages, if any.
    """

    valid: bool
    errors: list[str]


class ValidationDict(TypedDict):
    """
    Validation result for a complete data structure.

    Attributes
    ----------
    valid : bool
        Whether the entire data structure is valid.
    errors : list[str]
        List of error messages, if any.
    field_errors : dict[str, list[str]]
        Mapping of field names to their error messages.
    data : JSONData | None
        The validated data, if valid.
    """

    valid: bool
    errors: list[str]
    field_errors: dict[str, list[str]]
    data: JSONData | None


# SECTION: TYPE ALIASES ===================================================== #


type FieldErrors = dict[str, list[str]]
type FieldRuleInput = StrAnyMap | FieldRulesDict
type FieldRuleValidator = Callable[[Any, FieldRuleInput, list[str]], None]
type RuleCoercer[T] = Callable[[Any], T]
type RulesMap = Mapping[str, FieldRulesDict]


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _coerce_rule[T](
    rules: StrAnyMap,
    key: str,
    coercer: RuleCoercer[T],
    type_desc: str,
    errors: list[str],
) -> T | None:
    """
    Extract and coerce a rule value, recording an error.

    Returns None when the key is absent.

    Parameters
    ----------
    rules : StrAnyMap
        The rules dictionary.
    key : str
        The key to extract.
    coercer : RuleCoercer[T]
        Callable used to coerce the value.
    type_desc : str
        Description of the expected type for error messages.
    errors : list[str]
        List to append error messages to.

    Returns
    -------
    T | None
        The coerced value, or None if the key is absent.
    """
    if key not in rules:
        return None

    try:
        if (value := rules.get(key)) is None:
            return None
        return coercer(value)
    except (TypeError, ValueError):
        errors.append(f"Rule '{key}' must be {type_desc}")
        return None


def _field_result(
    errors: list[str],
) -> FieldValidationDict:
    """
    Build a stable field-validation result payload.

    Parameters
    ----------
    errors : list[str]
        List of error messages for the field.

    Returns
    -------
    FieldValidationDict
        Result with ``valid`` and a list of ``errors``.
    """
    return {
        'valid': not errors,
        'errors': errors,
    }


def _get_int_rule(
    rules: StrAnyMap,
    key: str,
    errors: list[str],
) -> int | None:
    """
    Extract and coerce an integer rule value, recording an error if invalid.

    Returns None when the key is absent.

    Parameters
    ----------
    rules : StrAnyMap
        The rules dictionary.
    key : str
        The key to extract.
    errors : list[str]
        List to append error messages to.

    Returns
    -------
    int | None
        The coerced integer value, or None if the key is absent.
    """
    coerced = _coerce_rule(rules, key, int, 'an integer', errors)

    return int(coerced) if coerced is not None else None


def _get_numeric_rule(
    rules: StrAnyMap,
    key: str,
    errors: list[str],
) -> float | None:
    """
    Extract and coerce a numeric rule value, recording an error if invalid.

    Returns None when the key is absent.

    Parameters
    ----------
    rules : StrAnyMap
        The rules dictionary.
    key : str
        The key to extract.
    errors : list[str]
        List to append error messages to.

    Returns
    -------
    float | None
        The coerced float value, or None if the key is absent.
    """
    coerced = _coerce_rule(rules, key, float, 'numeric', errors)

    return float(coerced) if coerced is not None else None


def _is_integer(
    value: Any,
) -> bool:
    """
    Return ``True`` if value is an integer but not a bool.

    Parameters
    ----------
    value : Any
        Value to test.

    Returns
    -------
    bool
        ``True`` if value is an integer, else ``False``.
    """
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    """
    Return True if value is an int/float but not a bool.

    Parameters
    ----------
    value : Any
        Value to test.

    Returns
    -------
    bool
        ``True`` if value is a number, else ``False``.
    """
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _type_matches(
    value: Any,
    expected: str,
) -> bool:
    """
    Check if a value matches an expected JSON-like type.

    Parameters
    ----------
    value : Any
        Value to test.
    expected : str
        Expected logical type name ('string', 'number', 'integer', 'boolean',
        'array', 'object').

    Returns
    -------
    bool
        ``True`` if the value matches the expected type; ``False`` if not.
    """
    match expected:
        case 'array':
            return isinstance(value, list)
        case 'boolean':
            return isinstance(value, bool)
        case 'integer':
            return _is_integer(value)
        case 'number':
            return _is_number(value)
        case 'object':
            return isinstance(value, dict)
        case 'string':
            return isinstance(value, str)
        case _:
            return False


def _validate_enum_rule(
    value: Any,
    rules: FieldRuleInput,
    errors: list[str],
) -> None:
    """Append enum-membership errors for one value."""
    if 'enum' not in rules:
        return

    enum_values = rules.get('enum')
    if not isinstance(enum_values, list):
        errors.append("Rule 'enum' must be a list")
        return
    if value not in enum_values:
        errors.append(f'Value {value} not in allowed values {enum_values}')


def _validate_record(
    record: Record,
    rules: RulesMap,
    idx: int | None = None,
) -> tuple[list[str], FieldErrors]:
    """
    Validate a single record against rules and return aggregated errors.

    Returns a tuple of (errors, field_errors) where errors are the flattened
    messages with field prefixes and field_errors maps field keys to messages.
    If idx is provided, the field keys are prefixed like ``"[i].field"``.

    Parameters
    ----------
    record : Record
        The record to validate.
    rules : RulesMap
        The field rules.
    idx : int | None, optional
        Optional index for prefixing field keys.

    Returns
    -------
    tuple[list[str], FieldErrors]
        A tuple of (errors, field_errors).
    """
    errors: list[str] = []
    field_errors: FieldErrors = {}

    for field, field_rules in rules.items():
        value = record.get(field)
        result = validate_field(value, field_rules)
        if result['valid']:
            continue
        field_key = field if idx is None else f'[{idx}].{field}'
        field_errors[field_key] = result['errors']
        errors.extend(f'{field_key}: {err}' for err in result['errors'])

    return errors, field_errors


def _validate_loaded_data(
    data: JSONData,
    rules: RulesMap,
) -> tuple[list[str], FieldErrors]:
    """Validate one loaded payload against the provided rules."""
    if isinstance(data, dict):
        return _validate_record(data, rules)
    return _validate_sequence(data, rules)


def _validate_numeric_rules(
    value: Any,
    rules: FieldRuleInput,
    errors: list[str],
) -> None:
    """Append numeric range errors for one value."""
    if not _is_number(value):
        return

    numeric_value = float(value)
    if (min_value := _get_numeric_rule(rules, 'min', errors)) is not None:
        if numeric_value < min_value:
            errors.append(f'Value {value} is less than minimum {min_value}')
    if (max_value := _get_numeric_rule(rules, 'max', errors)) is not None:
        if numeric_value > max_value:
            errors.append(f'Value {value} is greater than maximum {max_value}')


def _validate_pattern_rule(
    value: str,
    rules: FieldRuleInput,
    errors: list[str],
) -> None:
    """Append pattern-related errors for one string value."""
    if 'pattern' not in rules:
        return

    pattern = rules.get('pattern')
    if not isinstance(pattern, str):
        errors.append("Rule 'pattern' must be a string")
        return

    try:
        regex = re.compile(pattern)
    except re.error as exc:
        errors.append(f'Rule "pattern" is not a valid regex: {exc}')
        return

    if not regex.search(value):
        errors.append(f'Value does not match pattern {pattern}')


def _validation_result(
    *,
    data: JSONData | None,
    errors: list[str] | None = None,
    field_errors: FieldErrors | None = None,
) -> ValidationDict:
    """Build a stable validation result payload."""
    resolved_errors = list(errors or [])
    resolved_field_errors = dict(field_errors or {})
    return {
        'valid': not resolved_errors,
        'errors': resolved_errors,
        'field_errors': resolved_field_errors,
        'data': data,
    }


def _validate_sequence(
    data: list[Any],
    rules: RulesMap,
) -> tuple[list[str], FieldErrors]:
    """Validate a list payload against field rules."""
    errors: list[str] = []
    field_errors: FieldErrors = {}
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            key = f'[{i}]'
            msg = 'Item is not an object (expected dict)'
            errors.append(f'{key}: {msg}')
            field_errors.setdefault(key, []).append(msg)
            continue
        rec_errors, rec_field_errors = _validate_record(item, rules, i)
        errors.extend(rec_errors)
        field_errors.update(rec_field_errors)
    return errors, field_errors


def _validate_string_rules(
    value: Any,
    rules: FieldRuleInput,
    errors: list[str],
) -> None:
    """Append string length and pattern errors for one value."""
    if not isinstance(value, str):
        return

    value_length = len(value)
    if (min_length := _get_int_rule(rules, 'minLength', errors)) is not None:
        if value_length < min_length:
            errors.append(
                f'Length {value_length} is less than minimum {min_length}',
            )
    if (max_length := _get_int_rule(rules, 'maxLength', errors)) is not None:
        if value_length > max_length:
            errors.append(
                f'Length {value_length} is greater than maximum {max_length}',
            )
    _validate_pattern_rule(value, rules, errors)


def _validate_type_rule(
    value: Any,
    rules: FieldRuleInput,
    errors: list[str],
) -> None:
    """Append an error when the runtime value violates the declared type."""
    if not isinstance(expected_type := rules.get('type'), str):
        return
    if not _type_matches(value, expected_type):
        errors.append(
            f'Expected type {expected_type}, got {type(value).__name__}',
        )


_FIELD_RULE_VALIDATORS: tuple[FieldRuleValidator, ...] = (
    _validate_type_rule,
    _validate_numeric_rules,
    _validate_string_rules,
    _validate_enum_rule,
)


# SECTION: FUNCTIONS ======================================================== #


# -- Helpers -- #


def validate_field(
    value: Any,
    rules: FieldRuleInput,
) -> FieldValidationDict:
    """
    Validate a single value against field rules.

    Parameters
    ----------
    value : Any
        The value to validate. ``None`` is treated as missing.
    rules : StrAnyMap | FieldRulesDict
        Rule dictionary. Supported keys include ``required``, ``type``,
        ``min``, ``max``, ``minLength``, ``maxLength``, ``pattern``, and
        ``enum``.

    Returns
    -------
    FieldValidationDict
        Result with ``valid`` and a list of ``errors``.

    Notes
    -----
    If ``required`` is ``False`` or absent and the value is ``None``, the
    field is considered valid without further checks.
    """
    errors: list[str] = []

    # Required check (None is treated as missing).
    if bool(rules.get('required', False)) and value is None:
        errors.append('Field is required')
        return _field_result(errors)

    # If optional and missing, it's valid.
    if value is None:
        return _field_result(errors)

    for validator in _FIELD_RULE_VALIDATORS:
        validator(value, rules, errors)

    return _field_result(errors)


# -- Orchestration -- #


def validate(
    source: DataSourceArg,
    rules: RulesMap | None = None,
) -> ValidationDict:
    """
    Validate data against rules.

    Parameters
    ----------
    source : DataSourceArg
        Data source to validate.
    rules : RulesMap | None, optional
        Field rules keyed by field name. If ``None``, data is considered
        valid and returned unchanged.

    Returns
    -------
    ValidationDict
        Structured result with keys ``valid``, ``errors``, ``field_errors``,
        and ``data``. If loading fails, ``data`` is ``None`` and an error is
        reported in ``errors``.
    """
    try:
        data = _load_data(source)
    except (TypeError, ValueError) as exc:
        return _validation_result(
            data=None,
            errors=[f'Failed to load data: {exc}'],
        )

    if not rules:
        return _validation_result(data=data)

    errors, field_errors = _validate_loaded_data(data, rules)
    return _validation_result(
        data=data,
        errors=errors,
        field_errors=field_errors,
    )
