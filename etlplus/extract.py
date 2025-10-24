"""Data extraction module for ETLPlus.

This module provides functionality to extract data from various sources:
- Files (JSON, CSV, XML)
- Databases (via connection strings)
- REST APIs
"""

import json
import csv
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Any, Union
import requests


def extract_from_file(file_path: str, format: str = "json") -> Union[Dict, List]:
    """Extract data from a file.
    
    Args:
        file_path: Path to the file
        format: File format (json, csv, xml)
        
    Returns:
        Extracted data as dictionary or list
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If format is unsupported
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if format == "json":
        with open(path, "r") as f:
            return json.load(f)
            
    elif format == "csv":
        with open(path, "r") as f:
            reader = csv.DictReader(f)
            return list(reader)
            
    elif format == "xml":
        tree = ET.parse(path)
        root = tree.getroot()
        
        def element_to_dict(element):
            """Convert XML element to dictionary."""
            result = {}
            if element.text and element.text.strip():
                result["text"] = element.text.strip()
            for child in element:
                child_data = element_to_dict(child)
                if child.tag in result:
                    if not isinstance(result[child.tag], list):
                        result[child.tag] = [result[child.tag]]
                    result[child.tag].append(child_data)
                else:
                    result[child.tag] = child_data
            result.update(element.attrib)
            return result
            
        return {root.tag: element_to_dict(root)}
        
    else:
        raise ValueError(f"Unsupported format: {format}")


def extract_from_database(connection_string: str) -> List[Dict]:
    """Extract data from a database.
    
    Args:
        connection_string: Database connection string
        
    Returns:
        Extracted data as list of dictionaries
        
    Note:
        This is a placeholder implementation. In production, you would use
        database-specific libraries (psycopg2, pymysql, etc.)
    """
    # Placeholder implementation
    # In a real implementation, you would:
    # 1. Parse the connection string
    # 2. Connect to the database
    # 3. Execute queries
    # 4. Return results
    return [{
        "message": "Database extraction not yet implemented",
        "connection_string": connection_string,
        "note": "Install database-specific drivers to enable this feature"
    }]


def extract_from_api(url: str, **kwargs) -> Union[Dict, List]:
    """Extract data from a REST API.
    
    Args:
        url: API endpoint URL
        **kwargs: Additional arguments to pass to requests.get()
        
    Returns:
        Extracted data from API response
        
    Raises:
        requests.RequestException: If API request fails
    """
    response = requests.get(url, **kwargs)
    response.raise_for_status()
    
    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type:
        return response.json()
    else:
        return {"content": response.text, "content_type": content_type}


def extract(source_type: str, source: str, **kwargs) -> Union[Dict, List]:
    """Extract data from a source.
    
    Args:
        source_type: Type of source (file, database, api)
        source: Source location
        **kwargs: Additional arguments (e.g., format for files)
        
    Returns:
        Extracted data
        
    Raises:
        ValueError: If source_type is invalid
    """
    if source_type == "file":
        format = kwargs.get("format", "json")
        return extract_from_file(source, format)
    elif source_type == "database":
        return extract_from_database(source)
    elif source_type == "api":
        return extract_from_api(source, **kwargs)
    else:
        raise ValueError(f"Invalid source type: {source_type}")
