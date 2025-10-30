"""
ETLPlus Data Validation
=======================

Validate dicts and lists of dicts using simple, schema-like rules.

This module provides a very small validation primitive that is intentionally
runtime-friendly (no heavy schema engines) and pairs with ETLPlus' JSON-like
types. It focuses on clear error messages and predictable behavior.

Highlights
----------
- Centralized type map and helpers for clarity and reuse.
- Consistent error wording; field and item paths like ``[2].email``.
- Small, focused public API with ``load_data``, ``validate_field``,
  ``validate``.

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

import json
import re
from pathlib import Path
from typing import Any
from typing import Final
from typing import Literal
from typing import Mapping
from typing import TypedDict

from .types import JSONData
from .types import Record
from .types import Records
from .types import StrAnyMap
from .types import StrPath


# SECTION: PUBLIC API ======================================================= #


__all__ = [
    'FieldRules', 'FieldValidation', 'Validation',
    'load_data', 'validate_field', 'validate',
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


# SECTION: CLASSES ========================================================== #


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


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _get_int_rule(
    rules: Mapping[str, Any],
    key: str,
    errors: list[str],
) -> int | None:
    """
    Extract and coerce an integer rule value, recording an error if invalid.

    Returns None when the key is absent.
    """

    if key in rules:
        try:
            val = rules.get(key)
            return int(val) if val is not None else None
        except (TypeError, ValueError):
            errors.append(f"Rule '{key}' must be an integer")
    return None


def _get_numeric_rule(
    rules: Mapping[str, Any],
    key: str,
    errors: list[str],
) -> float | None:
    """
    Extract and coerce a numeric rule value, recording an error if invalid.

    Returns None when the key is absent.
    """

    if key in rules:
        try:
            val = rules.get(key)
            return float(val) if val is not None else None
        except (TypeError, ValueError):
            errors.append(f"Rule '{key}' must be numeric")
    return None


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


def _type_matches(value: Any, expected: str) -> bool:
    """
    Check if a value matches an expected JSON-like type.

    Parameters
    ----------
    value : Any
        Value to test.
    expected : {'string', 'number', 'integer', 'boolean', 'array', 'object'}
        Expected logical type name.

    Returns
    -------
    bool
        ``True`` if the value matches the expected type; ``False`` if not.
    """

    py_type = TYPE_MAP.get(expected)
    if py_type:
        return isinstance(value, py_type)
    return False


# SECTION: DATA LOADING ===================================================== #


def load_data(
    source: StrPath | JSONData,
) -> JSONData:
    """
    Load data from a file path, JSON string, or a direct object.

    Parameters
    ----------
    source : StrPath | JSONData
        Data source. If a path exists (str/Path/PathLike), JSON is read from
        the file. If a non-path string is given, it is parsed as JSON. Dicts or
        lists are returned unchanged.

    Returns
    -------
    JSONData
        Parsed object or list of objects.

    Raises
    ------
    ValueError
        If the input cannot be interpreted as a JSON object or array.
    """

    if isinstance(source, (dict, list)):
        return source

    # Try to load from file path if it exists.
    try:
        path = Path(source)
        if path.exists():
            with path.open(encoding='utf-8') as f:
                loaded = json.load(f)
            if isinstance(loaded, (dict, list)):
                return loaded
            raise ValueError(
                'JSON root must be an object or array when loading file',
            )
    except (OSError, json.JSONDecodeError):
        # Fall through and try to parse as a JSON string.
        pass

    # Try to parse as JSON string.
    try:
        text = source if isinstance(source, (str, bytes, bytearray)) \
            else str(source)
        loaded = json.loads(text)
        if isinstance(loaded, (dict, list)):
            return loaded
        raise ValueError(
            'JSON root must be an object or array when parsing string',
        )
    except json.JSONDecodeError as e:  # pragma: no cover
        raise ValueError(f'Invalid data source: {source}') from e


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


def validate(
    source: StrPath | Record | Records,
    rules: RulesMap | None = None,
) -> Validation:
    """
    Validate data against rules.

    Parameters
    ----------
    source : StrPath | Record | Records        Data source to validate.
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
        for field, field_rules in rules.items():
            value = data.get(field)
            result = validate_field(value, field_rules)
            if not result['valid']:
                field_errors[field] = result['errors']
                errors.extend(f'{field}: {err}' for err in result['errors'])

    elif isinstance(data, list):
        for i, item in enumerate(data):
            if not isinstance(item, dict):
                key = f'[{i}]'
                msg = 'Item is not an object (expected dict)'
                errors.append(f'{key}: {msg}')
                field_errors.setdefault(key, []).append(msg)
                continue
            for field, field_rules in rules.items():
                value = item.get(field)
                result = validate_field(value, field_rules)
                if not result['valid']:
                    field_key = f'[{i}].{field}'
                    field_errors[field_key] = result['errors']
                    errors.extend(
                        f'{field_key}: {err}' for err in result['errors']
                    )

    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'field_errors': field_errors,
        'data': data,
    }
