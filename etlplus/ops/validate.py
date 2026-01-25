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
- Small, focused public API with :func:`load_data`, :func:`validate_field`,
    :func:`validate`.

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
from collections.abc import Mapping
from typing import Any
from typing import Final
from typing import Literal
from typing import TypedDict

from ..types import JSONData
from ..types import Record
from ..types import StrAnyMap
from ..types import StrPath
from .load import load_data

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'FieldRules',
    'FieldValidation',
    'Validation',
    'validate_field',
    'validate',
]


# SECTION: CONSTANTS ======================================================== #


# Map the logical JSON-like type names to Python runtime types.
TYPE_MAP: Final[dict[str, type | tuple[type, ...]]] = {
    'string': str,
    'number': (int, float),
    'integer': int,
    'boolean': bool,
    'array': list,
    'object': dict,
}


# SECTION: TYPED DICTS ====================================================== #


class FieldRules(TypedDict, total=False):
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


class FieldValidation(TypedDict):
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


class Validation(TypedDict):
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


type RulesMap = Mapping[str, FieldRules]


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _coerce_rule(
    rules: StrAnyMap,
    key: str,
    coercer: type[int] | type[float],
    type_desc: str,
    errors: list[str],
) -> int | float | None:
    """
    Extract and coerce a rule value, recording an error.

    Returns None when the key is absent.

    Parameters
    ----------
    rules : StrAnyMap
        The rules dictionary.
    key : str
        The key to extract.
    coercer : type[int] | type[float]
        The type to coerce to (int or float).
    type_desc : str
        Description of the expected type for error messages.
    errors : list[str]
        List to append error messages to.

    Returns
    -------
    int | float | None
        The coerced value, or None if the key is absent.
    """
    if key not in rules:
        return None

    try:
        val = rules.get(key)
        if val is None:
            return None
        # Calling the type as a coercer is fine at runtime
        return coercer(val)  # type: ignore[call-arg]
    except (TypeError, ValueError):
        errors.append(f"Rule '{key}' must be {type_desc}")
        return None


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
    if expected == 'number':
        return _is_number(value)
    if expected == 'integer':
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == 'boolean':
        return isinstance(value, bool)

    py_type = TYPE_MAP.get(expected)
    return isinstance(value, py_type) if py_type else False


def _validate_record(
    record: Record,
    rules: RulesMap,
    idx: int | None = None,
) -> tuple[list[str], dict[str, list[str]]]:
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
    tuple[list[str], dict[str, list[str]]]
        A tuple of (errors, field_errors).
    """
    errors: list[str] = []
    field_errors: dict[str, list[str]] = {}

    for field, field_rules in rules.items():
        value = record.get(field)
        result = validate_field(value, field_rules)
        if result['valid']:
            continue
        field_key = field if idx is None else f'[{idx}].{field}'
        field_errors[field_key] = result['errors']
        errors.extend(f'{field_key}: {err}' for err in result['errors'])

    return errors, field_errors


# SECTION: FUNCTIONS ======================================================== #


# -- Helpers -- #


def validate_field(
    value: Any,
    rules: StrAnyMap | FieldRules,
) -> FieldValidation:
    """
    Validate a single value against field rules.

    Parameters
    ----------
    value : Any
        The value to validate. ``None`` is treated as missing.
    rules : StrAnyMap | FieldRules
        Rule dictionary. Supported keys include ``required``, ``type``,
        ``min``, ``max``, ``minLength``, ``maxLength``, ``pattern``, and
        ``enum``.

    Returns
    -------
    FieldValidation
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
        return {'valid': False, 'errors': errors}

    # If optional and missing, it's valid.
    if value is None:
        return {'valid': True, 'errors': []}

    # Type check.
    expected_type = rules.get('type')
    if isinstance(expected_type, str):
        if not _type_matches(value, expected_type):
            errors.append(
                f'Expected type {expected_type}, got {type(value).__name__}',
            )

    # Numeric range checks.
    if _is_number(value):
        min_v = _get_numeric_rule(rules, 'min', errors)
        if min_v is not None and float(value) < min_v:
            errors.append(f'Value {value} is less than minimum {min_v}')
        max_v = _get_numeric_rule(rules, 'max', errors)
        if max_v is not None and float(value) > max_v:
            errors.append(f'Value {value} is greater than maximum {max_v}')

    # String checks.
    if isinstance(value, str):
        min_len = _get_int_rule(rules, 'minLength', errors)
        if min_len is not None and len(value) < min_len:
            errors.append(
                f'Length {len(value)} is less than minimum {min_len}',
            )
        max_len = _get_int_rule(rules, 'maxLength', errors)
        if max_len is not None and len(value) > max_len:
            errors.append(
                f'Length {len(value)} is greater than maximum {max_len}',
            )
        if 'pattern' in rules:
            pattern = rules.get('pattern')
            if isinstance(pattern, str):
                try:
                    regex = re.compile(pattern)
                except re.error as e:
                    errors.append(f'Rule "pattern" is not a valid regex: {e}')
                else:
                    if not regex.search(value):
                        errors.append(
                            f'Value does not match pattern {pattern}',
                        )
            else:
                errors.append("Rule 'pattern' must be a string")

    # Enum check.
    if 'enum' in rules:
        enum_vals = rules.get('enum')
        if isinstance(enum_vals, list):
            if value not in enum_vals:
                errors.append(
                    f'Value {value} not in allowed values {enum_vals}',
                )
        else:
            errors.append("Rule 'enum' must be a list")

    return {'valid': len(errors) == 0, 'errors': errors}


# -- Orchestration -- #


def validate(
    source: StrPath | JSONData,
    rules: RulesMap | None = None,
) -> Validation:
    """
    Validate data against rules.

    Parameters
    ----------
    source : StrPath | JSONData
        Data source to validate.
    rules : RulesMap | None, optional
        Field rules keyed by field name. If ``None``, data is considered
        valid and returned unchanged.

    Returns
    -------
    Validation
        Structured result with keys ``valid``, ``errors``, ``field_errors``,
        and ``data``. If loading fails, ``data`` is ``None`` and an error is
        reported in ``errors``.
    """
    try:
        data = load_data(source)
    except ValueError as e:
        return {
            'valid': False,
            'errors': [f'Failed to load data: {e}'],
            'field_errors': {},
            'data': None,
        }

    if not rules:
        return {
            'valid': True,
            'errors': [],
            'field_errors': {},
            'data': data,
        }

    errors: list[str] = []
    field_errors: dict[str, list[str]] = {}

    if isinstance(data, dict):
        rec_errors, rec_field_errors = _validate_record(data, rules)
        errors.extend(rec_errors)
        field_errors.update(rec_field_errors)

    elif isinstance(data, list):
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

    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'field_errors': field_errors,
        'data': data,
    }
