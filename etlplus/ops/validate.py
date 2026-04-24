"""
:mod:`etlplus.ops.validate` module.

Validate dicts/lists with field rules and documents with external schemas.

This module provides a very small validation primitive that is intentionally
runtime-friendly (no heavy schema engines) and pairs with ETLPlus' JSON-like
types. It focuses on clear error messages and predictable behavior.

Highlights
----------
- Centralized type map and helpers for clarity and reuse.
- Consistent error wording; field and item paths like ``[2].email``.
- Small, focused public API with :func:`validate_field`, :func:`validate`,
    and :func:`validate_schema`.

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
from contextlib import suppress
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from typing import Literal
from typing import TypedDict

from ..file import File
from ..file import FileFormat
from ..utils import JsonCodec
from ..utils._types import JSONData
from ..utils._types import Record
from ..utils._types import StrAnyMap
from ._imports import import_frictionless
from ._imports import import_jsonschema
from ._imports import import_lxml_etree
from ._imports import import_yaml
from ._types import DataSourceArg
from .load import load_data as _load_data

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'validate_field',
    'validate_schema',
    'validate',
    # Typed Dicts
    'FieldRulesDict',
    'FieldValidationDict',
    'ValidationDict',
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
type RulesMap = Mapping[str, FieldRulesDict]
type SchemaFormat = Literal['frictionless', 'jsonschema', 'xsd']


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _coerce_rule[CoercedT](
    rules: StrAnyMap,
    key: str,
    coercer: Callable[[Any], CoercedT],
    type_desc: str,
    errors: list[str],
) -> CoercedT | None:
    """
    Extract and coerce a rule value, recording an error.

    Returns None when the key is absent.

    Parameters
    ----------
    rules : StrAnyMap
        The rules dictionary.
    key : str
        The key to extract.
    coercer : Callable[[Any], CoercedT]
        Callable used to coerce the value.
    type_desc : str
        Description of the expected type for error messages.
    errors : list[str]
        List to append error messages to.

    Returns
    -------
    CoercedT | None
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


def _format_jsonschema_path(
    path: Any,
) -> str | None:
    """Return a stable dotted/indexed path for one JSON Schema error."""
    parts: list[str] = []
    for part in path:
        if isinstance(part, int):
            parts.append(f'[{part}]')
            continue
        if not parts:
            parts.append(str(part))
            continue
        parts.append(f'.{part}')
    return ''.join(parts) or None


def _format_tabular_error_path(
    *,
    field_name: str | None,
    row_number: int | None,
) -> str | None:
    """Return a stable path for one tabular validation error."""
    if row_number is not None and field_name is not None:
        return f'row[{row_number}].{field_name}'
    if row_number is not None:
        return f'row[{row_number}]'
    if field_name is not None:
        return field_name
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


def _infer_structured_text_format(
    text: str,
) -> Literal['json', 'yaml']:
    """Infer JSON vs YAML from raw text."""
    return 'json' if text.lstrip().startswith(('{', '[')) else 'yaml'


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


def _normalize_frictionless_source_format(
    format_hint: str | None,
) -> FileFormat | None:
    """Normalize the supported CSV schema source formats."""
    if format_hint is None:
        return None

    normalized = FileFormat.coerce(format_hint)
    if normalized is not FileFormat.CSV:
        raise ValueError(
            f'Unsupported CSV schema source format: {format_hint}. '
            'Supported source formats: csv',
        )
    return normalized


def _normalize_jsonschema_format_hint(
    format_hint: str | None,
) -> FileFormat | None:
    """Normalize the supported JSON Schema source formats."""
    if format_hint is None:
        return None

    normalized = FileFormat.coerce(format_hint)
    if normalized not in (FileFormat.JSON, FileFormat.YAML):
        raise ValueError(
            f'Unsupported JSON Schema source format: {format_hint}. '
            'Supported source formats: json, yaml',
        )
    return normalized


def _parse_structured_text(
    text: str,
    *,
    format_hint: str | None,
    label: str,
) -> Any:
    """Parse raw JSON or YAML text for JSON Schema validation."""
    resolved_format = _normalize_jsonschema_format_hint(format_hint)
    if resolved_format is FileFormat.JSON:
        try:
            return JsonCodec.parse(text)
        except ValueError as exc:
            raise ValueError(f'Failed to parse {label} as JSON: {exc}') from exc

    if resolved_format is FileFormat.YAML or resolved_format is None:
        if resolved_format is None and _infer_structured_text_format(text) == 'json':
            try:
                return JsonCodec.parse(text)
            except ValueError:
                pass
        try:
            yaml = import_yaml()
            return yaml.safe_load(text)
        except RuntimeError:
            raise
        except Exception as exc:
            raise ValueError(f'Failed to parse {label} as YAML: {exc}') from exc

    raise ValueError(
        f'Unsupported JSON Schema {label.lower()} format: {format_hint}',
    )


def _load_jsonschema_document(
    value: str | Path,
    *,
    format_hint: str | None,
    label: str,
) -> Any:
    """Load a JSON Schema document or source instance from path or text."""
    resolved_format = _normalize_jsonschema_format_hint(format_hint)

    if isinstance(value, Path):
        try:
            return File(value, resolved_format).read()
        except FileNotFoundError as exc:
            raise ValueError(f'{label} not found: {value}') from exc

    candidate = Path(value)
    if candidate.exists():
        try:
            return File(candidate, resolved_format).read()
        except FileNotFoundError as exc:
            raise ValueError(f'{label} not found: {candidate}') from exc

    return _parse_structured_text(value, format_hint=format_hint, label=label)


def _resolve_existing_local_path(
    value: str | Path,
) -> Path | None:
    """Return an existing local path, if *value* points at one."""
    if isinstance(value, Path):
        return value

    with suppress(OSError, ValueError):
        candidate = Path(value)
        if candidate.exists():
            return candidate
    return None


def _resolve_local_path_or_text(
    value: str | Path,
) -> tuple[Path | None, bytes]:
    """Return a local path when one exists, else UTF-8 encoded text."""
    if isinstance(value, Path):
        return value, b''

    candidate = Path(value)
    if candidate.exists():
        return candidate, b''
    return None, value.encode('utf-8')


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


def _validate_jsonschema(
    source: str | Path,
    schema: str | Path,
    *,
    source_format: str | None = None,
) -> ValidationDict:
    """Validate one JSON or YAML document against one JSON Schema."""
    try:
        jsonschema = import_jsonschema()
    except RuntimeError as exc:
        return _validation_result(data=None, errors=[str(exc)])

    try:
        schema_doc = _load_jsonschema_document(
            schema,
            format_hint=None,
            label='Schema',
        )
    except (RuntimeError, ValueError) as exc:
        return _validation_result(data=None, errors=[str(exc)])

    try:
        validator_cls = jsonschema.validators.validator_for(schema_doc)
        validator_cls.check_schema(schema_doc)
    except jsonschema.exceptions.SchemaError as exc:
        return _validation_result(
            data=None,
            errors=[f'Invalid JSON Schema: {exc.message}'],
        )

    try:
        instance = _load_jsonschema_document(
            source,
            format_hint=source_format,
            label='Source',
        )
    except (RuntimeError, ValueError) as exc:
        return _validation_result(data=None, errors=[str(exc)])

    errors: list[str] = []
    field_errors: FieldErrors = {}
    validator = validator_cls(schema_doc)
    validation_errors = sorted(
        validator.iter_errors(instance),
        key=lambda error: list(error.absolute_path),
    )

    for error in validation_errors:
        path = _format_jsonschema_path(error.absolute_path)
        if path is None:
            errors.append(error.message)
            continue
        errors.append(f'{path}: {error.message}')
        field_errors.setdefault(path, []).append(error.message)

    return _validation_result(
        data=None,
        errors=errors,
        field_errors=field_errors,
    )


def _validate_frictionless(
    source: str | Path,
    schema: str | Path,
    *,
    source_format: str | None = None,
) -> ValidationDict:
    """Validate one CSV document against one Frictionless Table Schema."""
    try:
        frictionless = import_frictionless()
    except RuntimeError as exc:
        return _validation_result(data=None, errors=[str(exc)])

    frictionless_exception = frictionless.FrictionlessException

    try:
        schema_doc = _load_jsonschema_document(
            schema,
            format_hint=None,
            label='Schema',
        )
    except (RuntimeError, ValueError) as exc:
        return _validation_result(data=None, errors=[str(exc)])

    try:
        _normalize_frictionless_source_format(source_format)
    except ValueError as exc:
        return _validation_result(data=None, errors=[str(exc)])

    temp_dir: TemporaryDirectory[str] | None = None
    try:
        source_path = _resolve_existing_local_path(source)
        if source_path is None:
            temp_dir = TemporaryDirectory()
            source_path = Path(temp_dir.name) / 'source.csv'
            source_path.write_text(str(source), encoding='utf-8')

        schema_obj = frictionless.Schema.from_descriptor(schema_doc)
        resource = frictionless.Resource(
            path=source_path.name,
            basepath=str(source_path.parent),
            schema=schema_obj,
        )
        report = resource.validate()
    except (frictionless_exception, OSError, TypeError, ValueError) as exc:
        if temp_dir is not None:
            temp_dir.cleanup()
        return _validation_result(
            data=None,
            errors=[f'CSV schema validation failed: {exc}'],
        )

    errors: list[str] = []
    field_errors: FieldErrors = {}
    for task in report.tasks:
        for error in task.errors:
            descriptor = error.to_descriptor()
            message = str(descriptor.get('message') or error)
            path = _format_tabular_error_path(
                field_name=descriptor.get('fieldName'),
                row_number=descriptor.get('rowNumber'),
            )
            if path is None:
                errors.append(message)
                continue
            errors.append(f'{path}: {message}')
            field_errors.setdefault(path, []).append(message)

    if temp_dir is not None:
        temp_dir.cleanup()

    return _validation_result(
        data=None,
        errors=errors,
        field_errors=field_errors,
    )


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


def _validate_xsd(
    source: str | Path,
    schema: str | Path,
) -> ValidationDict:
    """Validate one XML document against one XSD schema."""
    try:
        etree = import_lxml_etree()
    except RuntimeError as exc:
        return _validation_result(data=None, errors=[str(exc)])

    source_path, source_text = _resolve_local_path_or_text(source)
    schema_path, schema_text = _resolve_local_path_or_text(schema)

    if source_path is not None and not source_path.exists():
        return _validation_result(
            data=None,
            errors=[f'XML not found: {source_path}'],
        )
    if schema_path is not None and not schema_path.exists():
        return _validation_result(
            data=None,
            errors=[f'XSD not found: {schema_path}'],
        )

    try:
        if schema_path is not None:
            with schema_path.open('rb') as handle:
                schema_doc = etree.parse(handle)
        else:
            schema_doc = etree.fromstring(schema_text)
        compiled_schema = etree.XMLSchema(schema_doc)

        if source_path is not None:
            with source_path.open('rb') as handle:
                xml_doc = etree.parse(handle)
        else:
            xml_doc = etree.fromstring(source_text)

        if compiled_schema.validate(xml_doc):
            return _validation_result(data=None)

        errors = [
            f'Line {error.line}: {error.message}' for error in compiled_schema.error_log
        ]
        if not errors:
            errors.append('XML failed schema validation')
        return _validation_result(data=None, errors=errors)
    except etree.XMLSchemaParseError as exc:
        return _validation_result(data=None, errors=[f'Invalid XSD: {exc}'])
    except etree.XMLSyntaxError as exc:
        return _validation_result(data=None, errors=[f'Invalid XML: {exc}'])


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


# SECTION: INTERNAL CONSTANTS =============================================== #


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
    rules : FieldRuleInput
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


def validate_schema(
    source: str | Path,
    schema: str | Path,
    *,
    schema_format: SchemaFormat | str | None = None,
    source_format: str | None = None,
) -> ValidationDict:
    """
    Validate one source document against one external schema.

    Parameters
    ----------
    source : str | Path
        XML source path or raw XML text.
    schema : str | Path
        Schema path or raw schema text.
    schema_format : SchemaFormat | str | None, optional
        Schema format override. Supported values are ``'xsd'``,
        ``'jsonschema'``, and ``'frictionless'``.
    source_format : str | None, optional
        Optional source payload format override for schema-based validation.

    Returns
    -------
    ValidationDict
        Structured validation result with ``valid``, ``errors``,
        ``field_errors``, and ``data``.
    """
    resolved_schema_format = (schema_format or 'xsd').lower()
    if resolved_schema_format == 'xsd':
        return _validate_xsd(source, schema)
    if resolved_schema_format == 'jsonschema':
        return _validate_jsonschema(
            source,
            schema,
            source_format=source_format,
        )
    if resolved_schema_format == 'frictionless':
        return _validate_frictionless(
            source,
            schema,
            source_format=source_format,
        )

    supported_formats = 'frictionless, jsonschema, xsd'
    if schema_format is None:
        resolved_label = 'None'
    else:
        resolved_label = str(schema_format)
    return _validation_result(
        data=None,
        errors=[
            f'Unsupported schema format: {resolved_label}. '
            f'Supported formats: {supported_formats}',
        ],
    )


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
