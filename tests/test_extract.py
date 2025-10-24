"""Tests for extract module."""

import json
import csv
import tempfile
import pytest
from pathlib import Path
from etlplus.extract import extract, extract_from_file, extract_from_api


def test_extract_from_json_file():
    """Test extracting data from JSON file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        test_data = {"name": "John", "age": 30}
        json.dump(test_data, f)
        temp_path = f.name
    
    try:
        result = extract_from_file(temp_path, "json")
        assert result == test_data
    finally:
        Path(temp_path).unlink()


def test_extract_from_csv_file():
    """Test extracting data from CSV file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "age"])
        writer.writeheader()
        writer.writerow({"name": "John", "age": "30"})
        writer.writerow({"name": "Jane", "age": "25"})
        temp_path = f.name
    
    try:
        result = extract_from_file(temp_path, "csv")
        assert len(result) == 2
        assert result[0]["name"] == "John"
        assert result[1]["name"] == "Jane"
    finally:
        Path(temp_path).unlink()


def test_extract_from_xml_file():
    """Test extracting data from XML file."""
    xml_content = """<?xml version="1.0"?>
    <person>
        <name>John</name>
        <age>30</age>
    </person>
    """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".xml", delete=False) as f:
        f.write(xml_content)
        temp_path = f.name
    
    try:
        result = extract_from_file(temp_path, "xml")
        assert "person" in result
        assert result["person"]["name"]["text"] == "John"
    finally:
        Path(temp_path).unlink()


def test_extract_file_not_found():
    """Test extracting from non-existent file."""
    with pytest.raises(FileNotFoundError):
        extract_from_file("/nonexistent/file.json", "json")


def test_extract_unsupported_format():
    """Test extracting with unsupported format."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("test")
        temp_path = f.name
    
    try:
        with pytest.raises(ValueError, match="Unsupported format"):
            extract_from_file(temp_path, "unsupported")
    finally:
        Path(temp_path).unlink()


def test_extract_wrapper_file():
    """Test extract wrapper with file type."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        test_data = {"test": "data"}
        json.dump(test_data, f)
        temp_path = f.name
    
    try:
        result = extract("file", temp_path, format="json")
        assert result == test_data
    finally:
        Path(temp_path).unlink()


def test_extract_invalid_source_type():
    """Test extract with invalid source type."""
    with pytest.raises(ValueError, match="Invalid source type"):
        extract("invalid", "source")
