"""Data validation module for ETLPlus.

This module provides functionality to validate data from various sources.
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Union


def load_data(source: Union[str, Dict, List]) -> Union[Dict, List]:
    """Load data from source (file or direct data).
    
    Args:
        source: Data source (file path, JSON string, or direct data)
        
    Returns:
        Loaded data
    """
    if isinstance(source, (dict, list)):
        return source
        
    # Try to load from file
    try:
        path = Path(source)
        if path.exists():
            with open(path, "r") as f:
                return json.load(f)
    except (OSError, json.JSONDecodeError):
        pass
    
    # Try to parse as JSON string
    try:
        return json.loads(source)
    except json.JSONDecodeError:
        raise ValueError(f"Invalid data source: {source}")


def validate_field(value: Any, rules: Dict) -> Dict[str, Any]:
    """Validate a field against rules.
    
    Args:
        value: Field value to validate
        rules: Validation rules
        
    Returns:
        Validation result with 'valid' and 'errors' keys
    """
    errors = []
    
    # Check required
    if rules.get("required", False) and value is None:
        errors.append("Field is required")
        return {"valid": False, "errors": errors}
    
    if value is None:
        return {"valid": True, "errors": []}
    
    # Check type
    expected_type = rules.get("type")
    if expected_type:
        type_map = {
            "string": str,
            "number": (int, float),
            "integer": int,
            "boolean": bool,
            "array": list,
            "object": dict,
        }
        if expected_type in type_map:
            if not isinstance(value, type_map[expected_type]):
                errors.append(f"Expected type {expected_type}, got {type(value).__name__}")
    
    # Check min/max for numbers
    if isinstance(value, (int, float)):
        if "min" in rules and value < rules["min"]:
            errors.append(f"Value {value} is less than minimum {rules['min']}")
        if "max" in rules and value > rules["max"]:
            errors.append(f"Value {value} is greater than maximum {rules['max']}")
    
    # Check length for strings
    if isinstance(value, str):
        if "minLength" in rules and len(value) < rules["minLength"]:
            errors.append(f"Length {len(value)} is less than minimum {rules['minLength']}")
        if "maxLength" in rules and len(value) > rules["maxLength"]:
            errors.append(f"Length {len(value)} is greater than maximum {rules['maxLength']}")
        if "pattern" in rules:
            import re
            if not re.match(rules["pattern"], value):
                errors.append(f"Value does not match pattern {rules['pattern']}")
    
    # Check enum values
    if "enum" in rules and value not in rules["enum"]:
        errors.append(f"Value {value} not in allowed values {rules['enum']}")
    
    return {"valid": len(errors) == 0, "errors": errors}


def validate(source: Union[str, Dict, List], rules: Dict = None) -> Dict[str, Any]:
    """Validate data against rules.
    
    Args:
        source: Data source to validate
        rules: Validation rules (optional)
        
    Returns:
        Validation result with 'valid', 'errors', and 'data' keys
    """
    try:
        data = load_data(source)
    except Exception as e:
        return {
            "valid": False,
            "errors": [f"Failed to load data: {str(e)}"],
            "data": None,
        }
    
    if not rules:
        # If no rules provided, just check if data is valid
        return {
            "valid": True,
            "errors": [],
            "data": data,
        }
    
    errors = []
    field_errors = {}
    
    # Validate based on data type
    if isinstance(data, dict):
        for field, field_rules in rules.items():
            value = data.get(field)
            result = validate_field(value, field_rules)
            if not result["valid"]:
                field_errors[field] = result["errors"]
                errors.extend([f"{field}: {err}" for err in result["errors"]])
    elif isinstance(data, list):
        # Validate each item in the list
        for i, item in enumerate(data):
            if isinstance(item, dict):
                for field, field_rules in rules.items():
                    value = item.get(field)
                    result = validate_field(value, field_rules)
                    if not result["valid"]:
                        field_key = f"[{i}].{field}"
                        field_errors[field_key] = result["errors"]
                        errors.extend([f"{field_key}: {err}" for err in result["errors"]])
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "field_errors": field_errors,
        "data": data,
    }
