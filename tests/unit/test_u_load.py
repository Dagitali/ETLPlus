"""
``tests.unit.test_u_load`` module.

Unit tests for ``etlplus.load``.

Notes
-----
- Validates load and load_data logic for dict, list, file, and error paths
    using temporary files and orchestrator dispatch.
- Uses parameterized cases for supported formats and error scenarios.
- Centralizes temporary file creation via fixture.
- Class-based suite for clarity and DRYness.
"""
import csv
import json

import pytest

from etlplus.load import load
from etlplus.load import load_data
from etlplus.load import load_to_file


# SECTION: TESTS ============================================================ #


class TestLoad:
    """
    Unit test suite for :func:`etlplus.load.load`.
    """

    def test_invalid_target_type(self):
        with pytest.raises(ValueError, match='Invalid DataConnectorType'):
            load({'test': 'data'}, 'invalid', 'target')

    def test_wrapper_database(self):
        test_data = {'test': 'data'}
        result = load(test_data, 'database', 'postgresql://localhost/testdb')
        assert result['status'] == 'not_implemented'

    def test_wrapper_file(
        self,
        tmp_path,
    ):
        output_path = tmp_path / 'output.json'
        test_data = {'test': 'data'}
        result = load(test_data, 'file', str(output_path), file_format='json')
        assert result['status'] == 'success'
        assert output_path.exists()


class TestLoadData:
    """
    Unit test suite for :func:`etlplus.load.load` and
    :func:`etlplus.load.load_data`.
    """

    @pytest.mark.parametrize(
        'input_data,expected',
        [
            ({'test': 'data'}, {'test': 'data'}),
            ([{'test': 'data'}], [{'test': 'data'}]),
        ],
    )
    def test_data_passthrough(
        self,
        input_data,
        expected,
    ):
        assert load_data(input_data) == expected

    def test_data_from_file(
        self,
        temp_json_file,
    ):
        test_data = {'test': 'data'}
        temp_path = temp_json_file(test_data)
        result = load_data(temp_path)
        assert result == test_data

    def test_data_from_json_string(self):
        json_str = '{"test": "data"}'
        result = load_data(json_str)
        assert result['test'] == 'data'

    # Already covered by test_load_data_passthrough
    def test_data_from_stdin(
        self,
        monkeypatch,
    ):
        class _FakeStdin:
            def read(self):
                return '{"items": [{"age": 30}, {"age": 20}]}'
        monkeypatch.setattr('sys.stdin', _FakeStdin())
        result = load_data('-')
        assert isinstance(result, dict)
        assert 'items' in result

    def test_data_invalid_source(self):
        with pytest.raises(ValueError, match='Invalid data source'):
            load_data('not a valid json string')


class TestLoadToFile:
    """
    Unit test suite for :func:`etlplus.load.load_to_file`.
    """

    def test_to_csv_file(
        self,
        tmp_path,
    ):
        output_path = tmp_path / 'output.csv'
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

    def test_to_csv_file_empty_list(
        self,
        tmp_path,
    ):
        output_path = tmp_path / 'output.csv'
        test_data = []
        result = load_to_file(test_data, str(output_path), 'csv')
        assert result['status'] == 'success'
        assert result['records'] == 0

    def test_to_csv_file_single_dict(
        self,
        tmp_path,
    ):
        output_path = tmp_path / 'output.csv'
        test_data = {'name': 'John', 'age': 30}
        result = load_to_file(test_data, str(output_path), 'csv')
        assert result['status'] == 'success'
        assert output_path.exists()

    def test_to_file_creates_directory(
        self,
        tmp_path,
    ):
        output_path = tmp_path / 'subdir' / 'output.json'
        test_data = {'test': 'data'}
        result = load_to_file(test_data, str(output_path), 'json')
        assert result['status'] == 'success'
        assert output_path.exists()

    def test_to_file_unsupported_format(
        self,
        tmp_path,
    ):
        output_path = tmp_path / 'output.txt'
        test_data = {'test': 'data'}
        with pytest.raises(ValueError, match='Invalid FileFormat'):
            load_to_file(test_data, str(output_path), 'unsupported')

    def test_to_json_file(
        self,
        tmp_path,
    ):
        output_path = tmp_path / 'output.json'
        test_data = {'name': 'John', 'age': 30}
        result = load_to_file(test_data, str(output_path), 'json')
        assert result['status'] == 'success'
        assert output_path.exists()
        with open(output_path, encoding='utf-8') as f:
            loaded_data = json.load(f)
        assert loaded_data == test_data
