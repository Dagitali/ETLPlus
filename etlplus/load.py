"""Data loading module for ETLPlus.

This module provides functionality to load data to various targets:
- Files (JSON, CSV)
- Databases (via connection strings)
- REST APIs
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any
from typing import Literal
from typing import TypeAlias

import requests

# -----------------------------
# Type aliases
# -----------------------------
JSONDict: TypeAlias = dict[str, Any]
JSONList: TypeAlias = list[JSONDict]
JSONData: TypeAlias = JSONDict | JSONList


# -----------------------------
# Data loading helper
# -----------------------------

def load_data(
    source: str | JSONData,
) -> JSONData:
    """Load data from a file path, JSON string, or direct object.

    Returns either a dict or a list of dicts. Raises ``ValueError`` if the
    input cannot be interpreted as a JSON object/array.
    """
    if isinstance(source, (dict, list)):
        return source

    # Try to load from file
    try:
        path = Path(source)
        if path.exists():
            with path.open('r', encoding='utf-8') as f:
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
# File target
# -----------------------------

def load_to_file(
    data: JSONData,
    file_path: str,
    fmt: Literal['json', 'csv'] = 'json',
) -> dict[str, Any]:
    """Persist data to a local file.

    Args:
        data: Data to write (dict or list of dicts)
        file_path: Target path
        fmt: File format ("json" or "csv")

    Returns:
        Result dictionary with status and record count.

    Raises:
        ValueError: If format is unsupported
    """
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if fmt == 'json':
        with path.open('w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        records = len(data) if isinstance(data, list) else 1
        return {
            'status': 'success',
            'message': f"Data loaded to {file_path}",
            'records': records,
        }

    if fmt == 'csv':
        rows: JSONList
        if isinstance(data, list):
            rows = [x for x in data if isinstance(x, dict)]
        else:
            rows = [data]

        if not rows:
            return {
                'status': 'success',
                'message': 'No data to write',
                'records': 0,
            }

        # Collect all unique field names across rows
        fieldnames_set: set[str] = set()
        for item in rows:
            fieldnames_set.update(item.keys())
        fieldnames: list[str] = sorted(fieldnames_set)

        with path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for item in rows:
                writer.writerow(item)

        return {
            'status': 'success',
            'message': f"Data loaded to {file_path}",
            'records': len(rows),
        }

    raise ValueError(f"Unsupported format: {fmt}")


# -----------------------------
# Database target (placeholder)
# -----------------------------

def load_to_database(
    data: JSONData,
    connection_string: str,
) -> dict[str, Any]:
    """Load data to a database (placeholder implementation)."""
    records = len(data) if isinstance(data, list) else 1
    return {
        'status': 'not_implemented',
        'message': 'Database loading not yet implemented',
        'connection_string': connection_string,
        'records': records,
        'note': 'Install database-specific drivers to enable this feature',
    }


# -----------------------------
# REST API target
# -----------------------------

def load_to_api(
    data: JSONData,
    url: str,
    method: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """Send data to a REST API.

    Args:
        data: Dict or list of dicts to send as JSON
        url: API endpoint URL
        method: HTTP method (POST, PUT, PATCH)
        **kwargs: Extra arguments forwarded to ``requests``

    Returns:
        Result dictionary including response payload or text.
    """
    match method.upper():
        case 'POST':
            response = requests.post(url, json=data, **kwargs)
        case 'PUT':
            response = requests.put(url, json=data, **kwargs)
        case 'PATCH':
            response = requests.patch(url, json=data, **kwargs)
        case _:
            raise ValueError(f"Unsupported HTTP method: {method}")

    response.raise_for_status()

    # Try JSON first, fall back to text
    try:
        payload: Any = response.json()
    except ValueError:
        payload = response.text

    return {
        'status': 'success',
        'status_code': response.status_code,
        'message': f"Data loaded to {url}",
        'response': payload,
        'records': len(data) if isinstance(data, list) else 1,
    }


# -----------------------------
# Orchestrator
# -----------------------------

def load(
    source: str | JSONDict | JSONList,
    target_type: Literal['file', 'database', 'api'],
    target: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """Load data to a target.

    Args:
        source: Data source to load
        target_type: One of "file", "database", or "api"
        target: Target location (file path, connection string, or URL)
        **kwargs: Additional args (e.g., fmt for files, method for APIs)

    Returns:
        Result dictionary with status.
    """
    data = load_data(source)

    if target_type == 'file':
        fmt = str(kwargs.pop('format', 'json')).lower()
        if fmt not in ('json', 'csv'):
            raise ValueError(f"Unsupported format: {fmt}")
        return load_to_file(data, target, fmt)  # type: ignore[arg-type]

    if target_type == 'database':
        return load_to_database(data, target)

    if target_type == 'api':
        method = str(kwargs.pop('method', 'POST'))
        return load_to_api(data, target, method, **kwargs)

    raise ValueError(f"Invalid target type: {target_type}")
