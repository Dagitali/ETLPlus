"""Tests for load module."""
import csv
import json
import tempfile
from pathlib import Path

import pytest

from etlplus.load import load
from etlplus.load import load_data
from etlplus.load import load_to_file


def test_load_data_from_dict():
    """Test loading data from dictionary."""
    data = {'test': 'data'}
    result = load_data(data)
    assert result == data


def test_load_data_from_list():
    """Test loading data from list."""
    data = [{'test': 'data'}]
    result = load_data(data)
    assert result == data


def test_load_data_from_json_string():
    """Test loading data from JSON string."""
    json_str = '{"test": "data"}'
    result = load_data(json_str)
    assert result['test'] == 'data'


def test_load_data_from_file():
    """Test loading data from file."""
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.json', delete=False,
    ) as f:
        test_data = {'test': 'data'}
        json.dump(test_data, f)
        temp_path = f.name

    try:
        result = load_data(temp_path)
        assert result == test_data
    finally:
        Path(temp_path).unlink()


def test_load_to_json_file():
    """Test loading data to JSON file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'output.json'
        test_data = {'name': 'John', 'age': 30}

        result = load_to_file(test_data, str(output_path), 'json')
        assert result['status'] == 'success'
        assert output_path.exists()

        with open(output_path) as f:
            loaded_data = json.load(f)
        assert loaded_data == test_data


def test_load_to_csv_file():
    """Test loading data to CSV file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'output.csv'
        test_data = [
            {'name': 'John', 'age': 30},
            {'name': 'Jane', 'age': 25},
        ]

        result = load_to_file(test_data, str(output_path), 'csv')
        assert result['status'] == 'success'
        assert output_path.exists()

        with open(output_path, newline='') as f:
            reader = csv.DictReader(f)
            loaded_data = list(reader)
        assert len(loaded_data) == 2
        assert loaded_data[0]['name'] == 'John'


def test_load_to_csv_file_single_dict():
    """Test loading single dictionary to CSV file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'output.csv'
        test_data = {'name': 'John', 'age': 30}

        result = load_to_file(test_data, str(output_path), 'csv')
        assert result['status'] == 'success'
        assert output_path.exists()


def test_load_to_csv_file_empty_list():
    """Test loading empty list to CSV file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'output.csv'
        test_data = []

        result = load_to_file(test_data, str(output_path), 'csv')
        assert result['status'] == 'success'
        assert result['records'] == 0


def test_load_to_file_creates_directory():
    """Test that load_to_file creates parent directories."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'subdir' / 'output.json'
        test_data = {'test': 'data'}

        result = load_to_file(test_data, str(output_path), 'json')
        assert result['status'] == 'success'
        assert output_path.exists()


def test_load_to_file_unsupported_format():
    """Test loading with unsupported format."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'output.txt'
        test_data = {'test': 'data'}

        with pytest.raises(ValueError, match='Unsupported format'):
            load_to_file(test_data, str(output_path), 'unsupported')


def test_load_wrapper_file():
    """Test load wrapper with file type."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'output.json'
        test_data = {'test': 'data'}

        result = load(test_data, 'file', str(output_path), format='json')
        assert result['status'] == 'success'
        assert output_path.exists()


def test_load_wrapper_database():
    """Test load wrapper with database type."""
    test_data = {'test': 'data'}
    result = load(test_data, 'database', 'postgresql://localhost/testdb')
    assert result['status'] == 'not_implemented'


def test_load_invalid_target_type():
    """Test load with invalid target type."""
    with pytest.raises(ValueError, match='Invalid target type'):
        load({'test': 'data'}, 'invalid', 'target')


def test_load_data_invalid_source():
    """Test loading data from invalid source."""
    with pytest.raises(ValueError, match='Invalid data source'):
        load_data('not a valid json string')
