"""
``tests.unit.test_u_load`` module.

Unit tests for ``etlplus.load``.

Notes
-----
- Uses temporary files for round-trip verification.
- Validates ``load_data`` passthrough semantics for dict/list inputs.
- Ensures error handling for unsupported targets.
"""
import csv
import json
import tempfile
from pathlib import Path

import pytest

from etlplus.load import load
from etlplus.load import load_data
from etlplus.load import load_to_file


# SECTION: TESTS =========================================================== #


def test_load_data_from_dict():
    """
    Load from a dictionary.

    Notes
    -----
    Ensures that objects passed directly are returned unchanged.
    """
    data = {'test': 'data'}
    result = load_data(data)
    assert result == data


def test_load_data_from_file():
    """
    Load from a JSON file path.

    Notes
    -----
    Writes a temporary JSON file and verifies round-trip parsing.
    """
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


def test_load_data_from_json_string():
    """
    Load from a JSON string.

    Notes
    -----
    Parses the JSON string and returns a mapping.
    """
    json_str = '{"test": "data"}'
    result = load_data(json_str)
    assert result['test'] == 'data'


def test_load_data_from_list():
    """
    Load from a list of dictionaries.

    Notes
    -----
    Ensures that lists passed directly are returned unchanged.
    """
    data = [{'test': 'data'}]
    result = load_data(data)
    assert result == data


def test_load_data_from_stdin(monkeypatch):
    """
    Load JSON from stdin when source is '-'.

    Notes
    -----
    Simulates piped stdin input for CLI usage like:
      etlplus ... | etlplus transform - --operations ...
    """
    class _FakeStdin:
        def read(self):
            return '{"items": [{"age": 30}, {"age": 20}]}'

    monkeypatch.setattr('sys.stdin', _FakeStdin())
    result = load_data('-')
    assert isinstance(result, dict)
    assert 'items' in result


def test_load_data_invalid_source():
    """
    Invalid JSON string raises ``ValueError`` when loading.
    """
    with pytest.raises(ValueError, match='Invalid data source'):
        load_data('not a valid json string')


def test_load_invalid_target_type():
    """
    Invalid target type raises ``ValueError``.
    """
    with pytest.raises(ValueError, match='Invalid DataConnectorType'):
        load({'test': 'data'}, 'invalid', 'target')


def test_load_to_json_file():
    """
    Write data to a JSON file.

    Notes
    -----
    Verifies file creation and content round-trip.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'output.json'
        test_data = {'name': 'John', 'age': 30}

        result = load_to_file(test_data, str(output_path), 'json')
        assert result['status'] == 'success'
        assert output_path.exists()

        with open(output_path, encoding='utf-8') as f:
            loaded_data = json.load(f)
        assert loaded_data == test_data


def test_load_to_csv_file():
    """
    Write a list of mappings to a CSV file.

    Notes
    -----
    Ensures header union and row writing are correct.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'output.csv'
        test_data = [
            {'name': 'John', 'age': 30},
            {'name': 'Jane', 'age': 25},
        ]

        result = load_to_file(test_data, str(output_path), 'csv')
        assert result['status'] == 'success'
        assert output_path.exists()

        with open(output_path, encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            loaded_data = list(reader)
        assert len(loaded_data) == 2
        assert loaded_data[0]['name'] == 'John'


def test_load_to_csv_file_single_dict():
    """
    Write a single mapping to a CSV file.

    Notes
    -----
    The writer should promote a mapping to a single-row CSV.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'output.csv'
        test_data = {'name': 'John', 'age': 30}

        result = load_to_file(test_data, str(output_path), 'csv')
        assert result['status'] == 'success'
        assert output_path.exists()


def test_load_to_csv_file_empty_list():
    """
    Write an empty list to a CSV file.

    Notes
    -----
    Should succeed and report zero records written.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'output.csv'
        test_data = []

        result = load_to_file(test_data, str(output_path), 'csv')
        assert result['status'] == 'success'
        assert result['records'] == 0


def test_load_to_file_creates_directory():
    """
    Ensure parent directories are created for file targets.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'subdir' / 'output.json'
        test_data = {'test': 'data'}

        result = load_to_file(test_data, str(output_path), 'json')
        assert result['status'] == 'success'
        assert output_path.exists()


def test_load_to_file_unsupported_format():
    """
    Unsupported format raises ``ValueError``.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'output.txt'
        test_data = {'test': 'data'}

        with pytest.raises(ValueError, match='Invalid FileFormat'):
            load_to_file(test_data, str(output_path), 'unsupported')


def test_load_wrapper_file():
    """
    Use the top-level ``load()`` to write a JSON file.

    Notes
    -----
    Verifies wrapper dispatch and file creation.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'output.json'
        test_data = {'test': 'data'}

        result = load(test_data, 'file', str(output_path), format='json')
        assert result['status'] == 'success'
        assert output_path.exists()


def test_load_wrapper_database():
    """
    Use the top-level ``load()`` with the database target.

    Notes
    -----
    Placeholder implementation should return ``not_implemented``.
    """
    test_data = {'test': 'data'}
    result = load(test_data, 'database', 'postgresql://localhost/testdb')
    assert result['status'] == 'not_implemented'
