"""
``tests.unit.test_u_extract`` module.

Unit tests for ``etlplus.extract``.

Notes
-----
- Uses temporary files to validate round-trip parsing.
- Ensures error handling for unsupported formats.
"""
import csv
import json
import tempfile
from pathlib import Path

import pytest

from etlplus.extract import extract
from etlplus.extract import extract_from_file


# SECTION: TESTS =========================================================== #


def test_extract_from_json_file():
    """
    Extract from a JSON file.

    Notes
    -----
    Writes a temporary JSON file and verifies round‑trip parsing.
    """
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.json', delete=False,
    ) as f:
        test_data = {'name': 'John', 'age': 30}
        json.dump(test_data, f)
        temp_path = f.name

    try:
        result = extract_from_file(temp_path, 'json')
        assert result == test_data
    finally:
        Path(temp_path).unlink()


def test_extract_from_csv_file():
    """
    Extract from a CSV file.

    Notes
    -----
    Writes two rows and verifies field parsing and row count.
    """
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.csv', delete=False, newline='',
    ) as f:
        writer = csv.DictWriter(f, fieldnames=['name', 'age'])
        writer.writeheader()
        writer.writerow({'name': 'John', 'age': '30'})
        writer.writerow({'name': 'Jane', 'age': '25'})
        temp_path = f.name

    try:
        result = extract_from_file(temp_path, 'csv')
        assert len(result) == 2
        assert result[0]['name'] == 'John'
        assert result[1]['name'] == 'Jane'
    finally:
        Path(temp_path).unlink()


def test_extract_from_xml_file():
    """
    Extract from an XML file.

    Notes
    -----
    Writes a small XML document and checks nested text extraction.
    """
    xml_content = """<?xml version="1.0"?>
    <person>
        <name>John</name>
        <age>30</age>
    </person>
    """
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.xml', delete=False,
    ) as f:
        f.write(xml_content)
        temp_path = f.name

    try:
        result = extract_from_file(temp_path, 'xml')
        assert 'person' in result
        assert result['person']['name']['text'] == 'John'
    finally:
        Path(temp_path).unlink()


def test_extract_file_not_found():
    """
    Attempt to extract data from a non-existent file.

    Notes
    -----
    Extracting a non-existent file should raise FileNotFoundError.
    """
    with pytest.raises(FileNotFoundError):
        extract_from_file('/nonexistent/file.json', 'json')


def test_extract_unsupported_format():
    """
    Attempt to extract data from a file with an unsupported format.

    Notes
    -----
    Unsupported format should raise ValueError.
    """
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.txt', delete=False,
    ) as f:
        f.write('test')
        temp_path = f.name

    try:
        with pytest.raises(ValueError, match='Invalid FileFormat'):
            extract_from_file(temp_path, 'unsupported')
    finally:
        Path(temp_path).unlink()


def test_extract_wrapper_file():
    """
    Orchestrator path for files.

    Notes
    -----
    Ensures the top‑level extract() dispatches to file extraction.
    """
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.json', delete=False,
    ) as f:
        test_data = {'test': 'data'}
        json.dump(test_data, f)
        temp_path = f.name

    try:
        result = extract('file', temp_path, file_format='json')
        assert result == test_data
    finally:
        Path(temp_path).unlink()


def test_extract_invalid_source_type():
    """
    Attempt to extract data with an invalid source type.

    Notes
    -----
    Invalid source type should raise ValueError.
    """
    with pytest.raises(ValueError, match='Invalid DataConnectorType'):
        extract('invalid', 'source')
