"""Data loading module for ETLPlus.

This module provides functionality to load data to various targets:
- Files (JSON, CSV)
- Databases (via connection strings)
- REST APIs
"""

import json
import csv
from pathlib import Path
from typing import Dict, List, Any, Union
import requests


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


def load_to_file(data: Union[Dict, List], file_path: str, format: str = "json") -> Dict:
    """Load data to a file.
    
    Args:
        data: Data to load
        file_path: Target file path
        format: File format (json, csv)
        
    Returns:
        Result dictionary with status
        
    Raises:
        ValueError: If format is unsupported
    """
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    if format == "json":
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        return {"status": "success", "message": f"Data loaded to {file_path}", "records": len(data) if isinstance(data, list) else 1}
        
    elif format == "csv":
        if not isinstance(data, list):
            data = [data]
        
        if not data:
            return {"status": "success", "message": "No data to write", "records": 0}
        
        # Get all unique keys from all dictionaries
        fieldnames = set()
        for item in data:
            if isinstance(item, dict):
                fieldnames.update(item.keys())
        fieldnames = sorted(fieldnames)
        
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for item in data:
                if isinstance(item, dict):
                    writer.writerow(item)
        
        return {"status": "success", "message": f"Data loaded to {file_path}", "records": len(data)}
        
    else:
        raise ValueError(f"Unsupported format: {format}")


def load_to_database(data: Union[Dict, List], connection_string: str) -> Dict:
    """Load data to a database.
    
    Args:
        data: Data to load
        connection_string: Database connection string
        
    Returns:
        Result dictionary with status
        
    Note:
        This is a placeholder implementation. In production, you would use
        database-specific libraries (psycopg2, pymysql, etc.)
    """
    # Placeholder implementation
    # In a real implementation, you would:
    # 1. Parse the connection string
    # 2. Connect to the database
    # 3. Insert/update data
    # 4. Return results
    records = len(data) if isinstance(data, list) else 1
    return {
        "status": "not_implemented",
        "message": "Database loading not yet implemented",
        "connection_string": connection_string,
        "records": records,
        "note": "Install database-specific drivers to enable this feature"
    }


def load_to_api(data: Union[Dict, List], url: str, method: str = "POST", **kwargs) -> Dict:
    """Load data to a REST API.
    
    Args:
        data: Data to load
        url: API endpoint URL
        method: HTTP method (POST, PUT, PATCH)
        **kwargs: Additional arguments to pass to requests
        
    Returns:
        Result dictionary with API response
        
    Raises:
        requests.RequestException: If API request fails
    """
    method = method.upper()
    
    if method == "POST":
        response = requests.post(url, json=data, **kwargs)
    elif method == "PUT":
        response = requests.put(url, json=data, **kwargs)
    elif method == "PATCH":
        response = requests.patch(url, json=data, **kwargs)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    response.raise_for_status()
    
    return {
        "status": "success",
        "status_code": response.status_code,
        "message": f"Data loaded to {url}",
        "response": response.json() if "application/json" in response.headers.get("content-type", "") else response.text,
        "records": len(data) if isinstance(data, list) else 1
    }


def load(source: Union[str, Dict, List], target_type: str, target: str, **kwargs) -> Dict:
    """Load data to a target.
    
    Args:
        source: Data source to load
        target_type: Type of target (file, database, api)
        target: Target location
        **kwargs: Additional arguments (e.g., format for files, method for APIs)
        
    Returns:
        Result dictionary with status
        
    Raises:
        ValueError: If target_type is invalid
    """
    data = load_data(source)
    
    if target_type == "file":
        format = kwargs.get("format", "json")
        return load_to_file(data, target, format)
    elif target_type == "database":
        return load_to_database(data, target)
    elif target_type == "api":
        method = kwargs.get("method", "POST")
        return load_to_api(data, target, method, **kwargs)
    else:
        raise ValueError(f"Invalid target type: {target_type}")
