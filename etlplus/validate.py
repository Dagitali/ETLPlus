"""Data validation module for ETLPlus.

This module provides functionality to validate data from various
sources.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from typing import Literal
from typing import Mapping
from typing import TypeAlias
from typing import TypedDict

# -----------------------------
# Type aliases and TypedDicts
# -----------------------------
JSONDict: TypeAlias = dict[str, Any]
JSONList: TypeAlias = list[JSONDict]
JSONData: TypeAlias = JSONDict | JSONList


class FieldRules(TypedDict, total=False):
    """Validation rules for a single field.

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
    valid: bool
    errors: list[str]


class Validation(TypedDict):
    valid: bool
    errors: list[str]
    field_errors: dict[str, list[str]]
    data: JSONData | None


# -----------------------------
# Data loading helpers
# -----------------------------


def load_data(
    source: str | JSONData,
) -> JSONData:
    """Load data from a file path, JSON string, or a direct object.

    Returns either a dict or a list of dicts.
    Raises ``ValueError`` if the input cannot be interpreted as a JSON
    object or array.
    """
    if isinstance(source, (dict, list)):
        return source

    # Try to load from file
    try:
        path = Path(source)
        if path.exists():
            with path.open() as f:
                loaded = json.load(f)
            if isinstance(loaded, (dict, list)):
                return loaded
            raise ValueError(
                'JSON root must be an object or array when loading file',
            )
    except (OSError, json.JSONDecodeError):
        # Fall through and try to parse as a JSON string
        pass

    # Try to parse as JSON string
    try:
        loaded = json.loads(source)
        if isinstance(loaded, (dict, list)):
            return loaded
        raise ValueError(
            'JSON root must be an object or array when parsing string',
        )
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid data source: {source}") from exc


# -----------------------------
# Validation primitives
# -----------------------------


def _type_matches(
    value: Any,
    expected: str,
) -> bool:
    type_map: dict[str, type | tuple[type, ...]] = {
        'string': str,
        'number': (int, float),
        'integer': int,
        'boolean': bool,
        'array': list,
        'object': dict,
    }
    py_type = type_map.get(expected)
    if py_type:
        return isinstance(value, py_type)
    return False


def validate_field(
    value: Any,
    rules: Mapping[str, Any] | FieldRules,
) -> FieldValidation:
    """Validate a single value against ``rules``.

    Returns a dict containing ``valid`` and ``errors`` (list of strings).
    """
    errors: list[str] = []

    # Required check (None is treated as missing)
    if bool(rules.get('required', False)) and value is None:
        errors.append('Field is required')
        return {'valid': False, 'errors': errors}

    # If optional and missing, it's valid
    if value is None:
        return {'valid': True, 'errors': []}

    # Type check
    expected_type = rules.get('type')
    if isinstance(expected_type, str):
        if not _type_matches(value, expected_type):
            errors.append(
                f"Expected type {expected_type}, got "
                f"{type(value).__name__}",
            )

    # Numeric range checks
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if 'min' in rules:
            try:
                min_v = float(rules['min'])  # type: ignore[assignment]
                if value < min_v:
                    errors.append(
                        f"Value {value} is less than minimum "
                        f"{min_v}",
                    )
            except (TypeError, ValueError):
                errors.append("Rule 'min' must be numeric")
        if 'max' in rules:
            try:
                max_v = float(rules['max'])  # type: ignore[assignment]
                if value > max_v:
                    errors.append(
                        f"Value {value} is greater than maximum "
                        f"{max_v}",
                    )
            except (TypeError, ValueError):
                errors.append("Rule 'max' must be numeric")

    # String checks
    if isinstance(value, str):
        if 'minLength' in rules:
            try:
                min_len = int(rules['minLength'])  # type: ignore[assignment]
                if len(value) < min_len:
                    errors.append(
                        f"Length {len(value)} is less than minimum "
                        f"{min_len}",
                    )
            except (TypeError, ValueError):
                errors.append("Rule 'minLength' must be an integer")
        if 'maxLength' in rules:
            try:
                max_len = int(rules['maxLength'])  # type: ignore[assignment]
                if len(value) > max_len:
                    errors.append(
                        f"Length {len(value)} is greater than maximum "
                        f"{max_len}",
                    )
            except (TypeError, ValueError):
                errors.append("Rule 'maxLength' must be an integer")
        if 'pattern' in rules:
            pattern = rules.get('pattern')
            if isinstance(pattern, str):
                if not re.search(pattern, value):
                    errors.append(f"Value does not match pattern {pattern}")
            else:
                errors.append("Rule 'pattern' must be a string")

    # Enum check
    if 'enum' in rules:
        enum_vals = rules.get('enum')
        if isinstance(enum_vals, list):
            if value not in enum_vals:
                errors.append(
                    f"Value {value} not in allowed values {enum_vals}",
                )
        else:
            errors.append("Rule 'enum' must be a list")

    return {'valid': len(errors) == 0, 'errors': errors}


# -----------------------------
# Top-level validation API
# -----------------------------


def validate(
    source: str | JSONDict | JSONList,
    rules: Mapping[str, FieldRules] | None = None,
) -> Validation:
    """Validate data against ``rules``.

    The returned dict always includes keys: ``valid``, ``errors``,
    ``field_errors``, and ``data``.
    """
    try:
        data = load_data(source)
    except Exception as exc:  # noqa: BLE001 - return structured error
        return {
            'valid': False,
            'errors': [f"Failed to load data: {exc}"],
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
                for err in result['errors']:
                    errors.append(f"{field}: {err}")

    elif isinstance(data, list):
        for i, item in enumerate(data):
            if not isinstance(item, dict):
                key = f"[{i}]"
                msg = 'Item is not an object (expected dict)'
                errors.append(f"{key}: {msg}")
                field_errors.setdefault(key, []).append(msg)
                continue
            for field, field_rules in rules.items():
                value = item.get(field)
                result = validate_field(value, field_rules)
                if not result['valid']:
                    field_key = f"[{i}].{field}"
                    field_errors[field_key] = result['errors']
                    for err in result['errors']:
                        errors.append(f"{field_key}: {err}")

    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'field_errors': field_errors,
        'data': data,
    }
