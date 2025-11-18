"""
``tests.unit.test_u_load`` module.

Unit tests for ``etlplus.load``.

Notes
-----
- Validates load and load_data logic for dict, list, file, and error paths
    using temporary files and orchestrator dispatch.
- Uses parameterized cases for supported formats and error scenarios.
- Centralizes temporary file creation via a fixture in conftest.py.
- Class-based suite for clarity and DRYness.
"""
import csv
import json
from pathlib import Path
from typing import Any
from typing import Callable
from typing import cast

import pytest

from etlplus.load import load
from etlplus.load import load_data
from etlplus.load import load_to_file


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestLoad:
    """
    Unit test suite for :func:`etlplus.load.load`.

    Notes
    -----
    - Tests error handling and supported target types.
    """

    def test_invalid_target_type(self) -> None:
        """Test error raised for invalid target type."""
        with pytest.raises(ValueError, match='Invalid DataConnectorType'):
            load({'test': 'data'}, 'invalid', 'target')

    @pytest.mark.parametrize(
        'target_type,target,expected_status',
        [
            ('database', 'postgresql://localhost/testdb', 'not_implemented'),
        ],
    )
    def test_wrapper_database(
        self,
        target_type: str,
        target: str,
        expected_status: str,
    ) -> None:
        """
        Test loading data to a database with a supported format.

        Parameters
        ----------
        target_type : str
            Type of target (e.g., 'database').
        target : str
            Target connection string.
        expected_status : str
            Expected status in result.
        """
        mock_data = {'test': 'data'}
        result = cast(
            dict[str, Any], load(
                mock_data, target_type, target,
            ),
        )
        assert result['status'] == expected_status

    def test_wrapper_file(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test loading data to a file with a supported format.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.

        Notes
        -----
        Supported format should not raise an error.
        """
        path = tmp_path / 'output.json'
        mock_data = {'test': 'data'}
        result = cast(
            dict[str, Any], load(
                mock_data, 'file', str(path), file_format='json',
            ),
        )
        assert result['status'] == 'success'
        assert path.exists()


@pytest.mark.unit
class TestLoadData:
    """
    Unit test suite for :func:`etlplus.load.load_data`.

    Notes
    -----
    - Tests passthrough, file, string, stdin, and error cases.
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
        input_data: dict[str, Any] | list[dict[str, Any]],
        expected: dict[str, Any] | list[dict[str, Any]],
    ) -> None:
        """
        Test passthrough for dict and list input.

        Parameters
        ----------
        input_data : dict[str, Any] | list[dict[str, Any]]
            Input data to load.
        expected : dict[str, Any] | list[dict[str, Any]]
            Expected output.
        """
        assert load_data(input_data) == expected

    def test_data_from_file(
        self,
        temp_json_file: Callable[[dict[str, Any]], str],
    ) -> None:
        """
        Test loading from a temporary JSON file.

        Parameters
        ----------
        temp_json_file : Callable[[dict[str, Any]], str]
            Fixture to create a temp JSON file.
        """
        mock_data = {'test': 'data'}
        temp_path = temp_json_file(mock_data)
        result = load_data(temp_path)
        assert result == mock_data

    def test_data_from_json_string(self) -> None:
        """
        Test loading from a JSON string.

        Notes
        -----
        Ensures JSON string is parsed to dict.
        """
        json_str = '{"test": "data"}'
        result = load_data(json_str)
        assert isinstance(result, dict)
        assert result['test'] == 'data'

    # Already covered by test_load_data_passthrough
    def test_data_from_stdin(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test loading from stdin using monkeypatch.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        """
        class _FakeStdin:
            def read(self):
                return '{"items": [{"age": 30}, {"age": 20}]}'
        monkeypatch.setattr('sys.stdin', _FakeStdin())
        result = load_data('-')
        assert isinstance(result, dict)
        assert 'items' in result

    def test_data_invalid_source(self) -> None:
        """
        Test error raised for invalid JSON source string.
        """
        with pytest.raises(ValueError, match='Invalid data source'):
            load_data('not a valid json string')


@pytest.mark.unit
class TestLoadToFile:
    """
    Unit test suite for :func:`etlplus.load.load_to_file`.

    Notes
    -----
    - Tests writing to CSV and JSON files,
        directory creation, and error handling.
    """

    def test_to_csv_file(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test writing a list of dicts to a CSV file.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.
        """
        path = tmp_path / 'output.csv'
        mock_data = [
            {'name': 'John', 'age': 30},
            {'name': 'Jane', 'age': 25},
        ]
        result: dict[str, Any] = load_to_file(mock_data, str(path), 'csv')
        assert result['status'] == 'success'
        assert path.exists()
        with open(path, encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            loaded_data: list[dict[str, Any]] = list(reader)
        assert len(loaded_data) == 2
        # loaded_data is a list of dicts, so index with integer then key
        first_row = loaded_data[0]
        assert isinstance(first_row, dict)
        assert first_row['name'] == 'John'

    def test_to_csv_file_empty_list(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test writing an empty list to a CSV file.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.
        """
        output_path = tmp_path / 'output.csv'
        mock_data: list[dict[str, Any]] = []
        result = load_to_file(mock_data, str(output_path), 'csv')
        assert result['status'] == 'success'
        assert result['records'] == 0

    def test_to_csv_file_single_dict(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test writing a single dict to a CSV file.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.
        """
        output_path = tmp_path / 'output.csv'
        mock_data = {'name': 'John', 'age': 30}
        result: dict[str, Any] = load_to_file(
            mock_data, str(output_path), 'csv',
        )
        assert result['status'] == 'success'
        assert output_path.exists()

    def test_to_file_creates_directory(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that parent directories are created for file targets.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.
        """
        output_path = tmp_path / 'subdir' / 'output.json'
        mock_data = {'test': 'data'}
        result: dict[str, Any] = load_to_file(
            mock_data, str(output_path), 'json',
        )
        assert result['status'] == 'success'
        assert output_path.exists()

    def test_to_file_unsupported_format(
        self, tmp_path: Path,
    ) -> None:
        """
        Test error raised for unsupported file format.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.
        """
        output_path = tmp_path / 'output.txt'
        mock_data = {'test': 'data'}
        with pytest.raises(ValueError, match='Invalid FileFormat'):
            load_to_file(mock_data, str(output_path), 'unsupported')

    def test_to_json_file(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test writing a dict to a JSON file.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.
        """
        output_path = tmp_path / 'output.json'
        mock_data = {'name': 'John', 'age': 30}
        result: dict[str, Any] = load_to_file(
            mock_data, str(output_path), 'json',
        )
        assert result['status'] == 'success'
        assert output_path.exists()
        with open(output_path, encoding='utf-8') as f:
            loaded_data = json.load(f)
        assert loaded_data == mock_data
