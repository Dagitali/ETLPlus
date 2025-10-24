"""Data transformation module for ETLPlus.

This module provides functionality to transform data.
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Union, Callable


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


def apply_filter(data: List[Dict], condition: Dict) -> List[Dict]:
    """Filter data based on conditions.
    
    Args:
        data: List of dictionaries to filter
        condition: Filter condition (e.g., {"field": "age", "op": "gt", "value": 18})
        
    Returns:
        Filtered data
    """
    field = condition.get("field")
    op = condition.get("op")
    value = condition.get("value")
    
    if not all([field, op, value is not None]):
        return data
    
    operators = {
        "eq": lambda x, y: x == y,
        "ne": lambda x, y: x != y,
        "gt": lambda x, y: x > y,
        "gte": lambda x, y: x >= y,
        "lt": lambda x, y: x < y,
        "lte": lambda x, y: x <= y,
        "in": lambda x, y: x in y,
        "contains": lambda x, y: y in x if isinstance(x, (list, str)) else False,
    }
    
    op_func = operators.get(op)
    if not op_func:
        return data
    
    return [item for item in data if field in item and op_func(item[field], value)]


def apply_map(data: List[Dict], mapping: Dict) -> List[Dict]:
    """Map/rename fields in data.
    
    Args:
        data: List of dictionaries to map
        mapping: Field mapping (e.g., {"old_name": "new_name"})
        
    Returns:
        Mapped data
    """
    result = []
    for item in data:
        new_item = {}
        for old_key, new_key in mapping.items():
            if old_key in item:
                new_item[new_key] = item[old_key]
        # Keep fields not in mapping
        for key, value in item.items():
            if key not in mapping:
                new_item[key] = value
        result.append(new_item)
    return result


def apply_select(data: List[Dict], fields: List[str]) -> List[Dict]:
    """Select specific fields from data.
    
    Args:
        data: List of dictionaries
        fields: List of fields to select
        
    Returns:
        Data with only selected fields
    """
    return [{field: item.get(field) for field in fields} for item in data]


def apply_sort(data: List[Dict], field: str, reverse: bool = False) -> List[Dict]:
    """Sort data by a field.
    
    Args:
        data: List of dictionaries to sort
        field: Field to sort by
        reverse: Sort in descending order if True
        
    Returns:
        Sorted data
    """
    return sorted(data, key=lambda x: x.get(field, ""), reverse=reverse)


def apply_aggregate(data: List[Dict], operation: Dict) -> Dict:
    """Aggregate data.
    
    Args:
        data: List of dictionaries to aggregate
        operation: Aggregation operation (e.g., {"field": "age", "func": "sum"})
        
    Returns:
        Aggregated result
    """
    field = operation.get("field")
    func = operation.get("func")
    
    if not field or not func:
        return {"error": "Invalid aggregation operation"}
    
    values = [item.get(field) for item in data if field in item and item[field] is not None]
    
    if func == "sum":
        return {f"{func}_{field}": sum(values)}
    elif func == "avg":
        return {f"{func}_{field}": sum(values) / len(values) if values else 0}
    elif func == "min":
        return {f"{func}_{field}": min(values) if values else None}
    elif func == "max":
        return {f"{func}_{field}": max(values) if values else None}
    elif func == "count":
        return {f"{func}_{field}": len(values)}
    else:
        return {"error": f"Unknown aggregation function: {func}"}


def transform(source: Union[str, Dict, List], operations: Dict = None) -> Union[Dict, List]:
    """Transform data based on operations.
    
    Args:
        source: Data source to transform
        operations: Transformation operations
        
    Returns:
        Transformed data
        
    Example operations:
        {
            "filter": {"field": "age", "op": "gt", "value": 18},
            "map": {"old_name": "new_name"},
            "select": ["name", "age"],
            "sort": {"field": "name", "reverse": False},
            "aggregate": {"field": "age", "func": "avg"}
        }
    """
    data = load_data(source)
    
    if not operations:
        return data
    
    # Convert single dict to list for uniform processing
    is_single_dict = isinstance(data, dict)
    if is_single_dict:
        data = [data]
    
    # Apply transformations in order
    if "filter" in operations and isinstance(data, list):
        data = apply_filter(data, operations["filter"])
    
    if "map" in operations and isinstance(data, list):
        data = apply_map(data, operations["map"])
    
    if "select" in operations and isinstance(data, list):
        data = apply_select(data, operations["select"])
    
    if "sort" in operations and isinstance(data, list):
        sort_config = operations["sort"]
        if isinstance(sort_config, dict):
            data = apply_sort(data, sort_config.get("field"), sort_config.get("reverse", False))
        else:
            data = apply_sort(data, sort_config)
    
    if "aggregate" in operations and isinstance(data, list):
        return apply_aggregate(data, operations["aggregate"])
    
    # Convert back to single dict if input was single dict
    if is_single_dict and len(data) == 1:
        return data[0]
    
    return data
