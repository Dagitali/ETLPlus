"""
:mod:`tests.unit.ops.test_u_ops_load` module.

Unit tests for :mod:`etlplus.ops.load`.

Notes
-----
- Validates load and load_data logic for dict, list, file, and error paths
    using temporary files and orchestrator dispatch.
- Uses parameterized cases for supported formats and error scenarios.
- Centralizes temporary file creation via a fixture in conftest.py.
- Class-based suite for clarity and DRYness.
"""

from __future__ import annotations

import csv
import json as js
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import cast

import pytest

from etlplus.api import HttpMethod
from etlplus.connector import DataConnectorType
from etlplus.ops.load import _parse_json_string
from etlplus.ops.load import load
from etlplus.ops.load import load_data
from etlplus.ops.load import load_to_api
from etlplus.ops.load import load_to_database
from etlplus.ops.load import load_to_file
from etlplus.types import JSONData

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


@dataclass(slots=True)
class _CallRecord:
    """Record of an HTTP method call in the stub session."""

    method: str
    url: str
    json: object
    timeout: float
    kwargs: dict[str, Any]


class _StubResponse:
    """Minimal HTTP response stub for API load tests."""

    def __init__(self, payload: object) -> None:
        self._payload = payload
        self.status_code = 200
        self.text = 'ok'

    def json(self) -> object:
        """Return the stubbed JSON payload."""
        return self._payload

    def raise_for_status(self) -> None:
        """No-op for status raising in stub."""
        return None


class _StubSession:
    """Capture HTTP method calls to assert ``load_to_api`` behavior."""

    def __init__(self, payload: object | None = None) -> None:
        self.calls: list[_CallRecord] = []
        self.payload = payload or {'ok': True}

    def post(
        self,
        url: str,
        *,
        json: object,
        timeout: float,
        **kwargs: Any,
    ) -> _StubResponse:  # noqa: ANN001
        """Capture POST call details."""
        record = _CallRecord(
            method='post',
            url=url,
            json=json,
            timeout=timeout,
            kwargs=dict(kwargs),
        )
        self.calls.append(record)
        return _StubResponse(self.payload)

    def put(
        self,
        url: str,
        *,
        json: object,
        timeout: float,
        **kwargs: Any,
    ) -> _StubResponse:  # noqa: ANN001
        """Capture PUT call details."""
        record = _CallRecord(
            method='put',
            url=url,
            json=json,
            timeout=timeout,
            kwargs=dict(kwargs),
        )
        self.calls.append(record)
        return _StubResponse(self.payload)


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestLoad:
    """
    Unit test suite for :func:`etlplus.ops.load.load`.

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
            (
                'database',
                'postgresql://localhost/testdb',
                'not_implemented',
            ),
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
            dict[str, Any],
            load(
                mock_data,
                target_type,
                target,
            ),
        )
        assert result['status'] == expected_status

    @pytest.mark.parametrize(
        'file_format,write,expected_data',
        [
            (
                'json',
                lambda p, d: js.dump(
                    d,
                    open(p, 'w', encoding='utf-8'),
                ),
                {'test': 'data'},
            ),
        ],
    )
    def test_wrapper_file(
        self,
        tmp_path: Path,
        file_format: str,
        write: Callable[[str, Any], None],
        expected_data: Any,
    ) -> None:
        """
        Test loading data to a file with a supported format.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.
        file_format : str
            File format of the data.
        write : Callable[[str, Any], None]
            Function to write data to the file.
        expected_data : Any
            Expected data to write and read.

        Notes
        -----
        Supported format should not raise an error.
        """
        path = tmp_path / f'output.{file_format}'
        write(str(path), expected_data)
        result = cast(
            dict[str, Any],
            load(
                expected_data,
                'file',
                str(path),
                file_format=file_format,
            ),
        )
        assert result['status'] == 'success'
        assert path.exists()

    @pytest.mark.parametrize(
        'exc_type,call,args,err_msg',
        [
            (
                ValueError,
                load,
                [
                    {'test': 'data'},
                    'file',
                    'output.unsupported',
                    'unsupported',
                ],
                'Invalid FileFormat',
            ),
        ],
    )
    def test_wrapper_file_unsupported_format(
        self,
        exc_type: type[Exception],
        call: Callable,
        args: list[Any],
        err_msg: str,
    ) -> None:
        """
        Test error raised for unsupported file format.

        Parameters
        ----------
        exc_type : type[Exception]
            Expected exception type.
        call : Callable
            Function to call.
        args : list[Any]
            Arguments to pass to the function.
        err_msg : str
            Expected error message substring.
        """
        with pytest.raises(exc_type) as e:
            call(*args)
        assert err_msg in str(e.value)


@pytest.mark.unit
class TestLoadErrors:
    """
    Unit test suite for ``etlplus.ops.load`` function errors.

    Notes
    -----
    - Tests error handling for load and load_data.
    """

    @pytest.mark.parametrize(
        'exc_type,call,args,err_msg',
        [
            (
                ValueError,
                load_data,
                ['/nonexistent/file.json'],
                'Invalid data source',
            ),
            (
                ValueError,
                load,
                ['/nonexistent/file.json', 'invalid', 'source', 'json'],
                'Invalid data source',
            ),
        ],
    )
    def test_error_cases(
        self,
        exc_type: type[Exception],
        call: Callable[..., Any],
        args: list[Any],
        err_msg: str | None,
    ) -> None:
        """
        Test parametrized error case tests for load/load_data.

        Parameters
        ----------
        exc_type : type[Exception]
            Expected exception type.
        call : Callable[..., Any]
            Function to call.
        args : list[Any]
            Arguments to pass to the function.
        err_msg : str | None
            Expected error message substring, if applicable.
        """
        with pytest.raises(exc_type) as exc:
            call(*args)
        if err_msg:
            assert err_msg in str(exc.value)


@pytest.mark.unit
class TestLoadData:
    """
    Unit test suite for :func:`etlplus.ops.load.load_data`.

    Notes
    -----
    - Tests passthrough, file, string, stdin, and error cases.
    """

    @pytest.mark.parametrize(
        'input_data,expected_output',
        [
            ({'test': 'data'}, {'test': 'data'}),
            ([{'test': 'data'}], [{'test': 'data'}]),
        ],
    )
    def test_data_passthrough(
        self,
        input_data: dict[str, Any] | list[dict[str, Any]],
        expected_output: dict[str, Any] | list[dict[str, Any]],
    ) -> None:
        """
        Test passthrough for dict and list input.

        Parameters
        ----------
        input_data : dict[str, Any] | list[dict[str, Any]]
            Input data to load.
        expected_output : dict[str, Any] | list[dict[str, Any]]
            Expected output.
        """
        assert load_data(input_data) == expected_output

    def test_data_from_file(
        self,
        temp_json_file: Callable[[JSONData], Path],
    ) -> None:
        """
        Test loading from a temporary JSON file.

        Parameters
        ----------
        temp_json_file : Callable[[JSONData], Path]
            Fixture to create a temp JSON file in a pytest-managed directory.
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
        Test loading from STDIN using monkeypatch.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        """

        class _FakeStdin:
            def read(self) -> str:
                """Simulate reading JSON data from stdin."""
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
    Unit test suite for :func:`etlplus.ops.load.load_to_file`.

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
        first_row: dict[str, Any] = loaded_data[0]
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
            mock_data,
            str(output_path),
            'csv',
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
            mock_data,
            str(output_path),
            'json',
        )
        assert result['status'] == 'success'
        assert output_path.exists()

    def test_to_file_unsupported_format(
        self,
        tmp_path: Path,
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
            mock_data,
            str(output_path),
            'json',
        )
        assert result['status'] == 'success'
        assert output_path.exists()
        with open(output_path, encoding='utf-8') as f:
            loaded_data = js.load(f)
        assert loaded_data == mock_data


@pytest.mark.unit
class TestLoadToApi:
    """Unit tests for :func:`etlplus.ops.load.load_to_api`."""

    def test_load_to_api_success(self) -> None:
        """Test that payload and metadata are returned through stub session."""

        session = _StubSession({'ok': True})
        data = [{'name': 'Ada'}]

        result = load_to_api(
            data,
            'https://example.test/api',
            'post',
            session=session,
            headers={'X-Test': '1'},
        )

        assert result['status'] == 'success'
        assert result['records'] == 1
        assert result['method'] == 'POST'
        api_calls: list[_CallRecord] = session.calls
        assert api_calls
        first_call: _CallRecord = api_calls[0]
        assert first_call.kwargs['headers'] == {'X-Test': '1'}


@pytest.mark.unit
class TestLoadToDatabase:
    """Unit tests for :func:`etlplus.ops.load.load_to_database`."""

    def test_load_to_api_requires_callable(self) -> None:
        """Missing HTTP method on custom session should raise TypeError."""

        class _BrokenSession:
            pass

        with pytest.raises(TypeError):
            load_to_api(
                {'ok': True},
                'https://example.test/api',
                HttpMethod.POST,
                session=_BrokenSession(),
            )

    def test_load_to_database_returns_note(self) -> None:
        """Placeholder implementation should echo the connection string."""

        data = [{'name': 'Ada'}]
        result = load_to_database(data, 'sqlite:///tmp.db')

        assert result['status'] == 'not_implemented'
        assert result['records'] == 1
        assert 'sqlite' in result['connection_string']


@pytest.mark.unit
class TestParseJsonString:
    """Unit tests for :func:`etlplus.ops.load._parse_json_string`."""

    def test_parse_invalid_root_raises(self) -> None:
        """Only dicts or lists of dicts are accepted."""

        with pytest.raises(ValueError):
            _parse_json_string('"plain"')

    def test_parse_list_with_non_dicts_raises(self) -> None:
        """Mixed arrays should raise ValueError."""

        with pytest.raises(ValueError):
            _parse_json_string('[{"ok": 1}, 3]')


@pytest.mark.unit
class TestLoadApiOrchestrator:
    """
    Unit tests that ensure :func:`etlplus.ops.load.load` delegates to API
    loader.
    """

    def test_load_api_with_default_method(self) -> None:
        """Test :func:`load` defaulting to POST when API method omitted."""

        session = _StubSession()
        result = load(
            {'name': 'api'},
            DataConnectorType.API,
            'https://example.test/api',
            session=session,
        )

        result_dict = cast(dict[str, Any], result)
        assert result_dict['status'] == 'success'
        calls: list[_CallRecord] = session.calls
        assert calls
        first_call: _CallRecord = calls[0]
        assert first_call.method == 'post'

    def test_load_api_with_explicit_method(self) -> None:
        """Test :func:`load` honoring custom :class:`HttpMethod`."""

        session = _StubSession()
        load(
            {'name': 'api'},
            DataConnectorType.API,
            'https://example.test/api',
            method=HttpMethod.PUT,
            session=session,
        )

        calls: list[_CallRecord] = session.calls
        assert calls
        first_call: _CallRecord = calls[0]
        assert first_call.method == 'put'
