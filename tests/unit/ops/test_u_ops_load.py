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
import importlib
import json
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
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
from etlplus.utils._types import JSONData
from etlplus.utils._types import JSONDict
from tests.unit.ops.pytest_ops_support import ApiSession
from tests.unit.ops.pytest_ops_support import JsonResponse
from tests.unit.ops.pytest_ops_support import write_json_payload

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: IMPORTS ========================================================== #


load_mod = importlib.import_module('etlplus.ops.load')


# SECTION: TESTS ============================================================ #


class TestLoad:
    """
    Unit tests for :func:`etlplus.ops.load.load`.

    Notes
    -----
    - Tests error handling and supported target types.
    """

    def test_invalid_target_type(self) -> None:
        """Test error raised for invalid target type."""
        with pytest.raises(ValueError, match='Invalid DataConnectorType'):
            load({'test': 'data'}, 'invalid', 'target')

    @pytest.mark.parametrize(
        ('target_type', 'target', 'expected_status'),
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
        ('file_format', 'write', 'expected_data'),
        [
            (
                'json',
                write_json_payload,
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

    def test_wrapper_file_unsupported_format(self) -> None:
        """
        Test error raised for unsupported file format.
        """
        with pytest.raises(ValueError, match='Invalid FileFormat'):
            load({'test': 'data'}, 'file', 'output.unsupported', 'unsupported')


class TestLoadData:
    """
    Unit tests for :func:`etlplus.ops.load.load_data`.

    Notes
    -----
    - Tests passthrough, file, string, STDIN, and error cases.
    """

    def test_data_from_existing_path_falls_back_to_json_string(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that existing-path read failures falls back to raw JSON parsing.
        """

        class _FailingFile:
            def __init__(self, *_args: object, **_kwargs: object) -> None:
                return None

            def read(self) -> JSONData:
                """Simulate a file read failure by raising an error."""
                raise ValueError('cannot read as file')

        monkeypatch.setattr(load_mod.Path, 'exists', lambda _self: True)
        monkeypatch.setattr(load_mod, 'File', _FailingFile)

        assert load_data('{"ok": true}') == {'ok': True}

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

    def test_data_from_remote_json_uri(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test loading JSON input from a remote file URI."""
        captured: dict[str, Any] = {}

        class _FakeFile:
            def __init__(
                self,
                path: str,
                file_format: object = None,
            ) -> None:
                captured['path'] = path
                captured['file_format'] = file_format

            def exists(self) -> bool:
                return True

            def read(self) -> JSONData:
                return {'remote': True}

        monkeypatch.setattr(load_mod, 'File', _FakeFile)

        result = load_data('s3://bucket/input.json')

        assert result == {'remote': True}
        assert captured['path'] == 's3://bucket/input.json'
        assert captured['file_format'] == load_mod.FileFormat.JSON

    # TODO: Already covered by test_load_data_passthrough.
    # TODO: Consider removing or refactoring to avoid redundancy.
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
                """Simulate reading JSON data from STDIN."""
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

    @pytest.mark.parametrize(
        ('input_data', 'expected_output'),
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

    def test_load_data_rejects_unsupported_source_type(self) -> None:
        """Test that unsupported source types raises :class:`TypeError`."""
        with pytest.raises(TypeError, match='source must be'):
            load_data(cast(Any, 123))


class TestLoadErrors:
    """
    Unit tests for :mod:`etlplus.ops.load` function errors.

    Notes
    -----
    - Tests error handling for load and load_data.
    """

    @pytest.mark.parametrize(
        ('call', 'args', 'err_msg'),
        [
            (
                load_data,
                ['/nonexistent/file.json'],
                'Invalid data source',
            ),
            (
                load,
                ['/nonexistent/file.json', 'invalid', 'source', 'json'],
                'Invalid data source',
            ),
        ],
    )
    def test_error_cases(
        self,
        call: Callable[..., Any],
        args: list[Any],
        err_msg: str,
    ) -> None:
        """
        Test parametrized error case tests for load/load_data.

        Parameters
        ----------
        call : Callable[..., Any]
            Function to call.
        args : list[Any]
            Arguments to pass to the function.
        err_msg : str
            Expected error message substring.
        """
        with pytest.raises(ValueError, match=err_msg):
            call(*args)


class TestLoadToApi:
    """Unit tests for :func:`etlplus.ops.load.load_to_api`."""

    def test_load_to_api_success(self) -> None:
        """Test that payload and metadata are returned through stub session."""

        session = ApiSession({'ok': True})
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
        (first_call,) = session.calls
        assert first_call.kwargs['headers'] == {'X-Test': '1'}

    def test_load_to_api_env_requires_url(self) -> None:
        """
        Test that missing URL in normalized API env raises :class:`ValueError`.
        """
        with pytest.raises(ValueError, match='API target missing "url"'):
            load_mod._load_to_api_env({'method': 'post'}, {})

    def test_load_to_api_env_includes_headers_timeout_session_and_extras(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that normalized API env forwards headers and request kwargs.
        """
        captured: dict[str, Any] = {}

        def _request(url: str, **kwargs: Any) -> JsonResponse:
            """Stub request function that captures URL and kwargs."""
            captured['url'] = url
            captured['kwargs'] = kwargs
            return JsonResponse(status_code=201)

        def _build_request_call(
            env: dict[str, Any],
            error_message: str,
            default_method: HttpMethod,
            json_data: object = None,
        ) -> SimpleNamespace:
            _ = error_message, default_method
            return SimpleNamespace(
                url=env['url'],
                request_callable=_request,
                timeout=9.5,
                http_method=HttpMethod.PUT,
                kwargs={
                    'headers': {'X-Test': '1'},
                    'json': json_data,
                    'verify': False,
                },
            )

        monkeypatch.setattr(
            load_mod,
            'build_request_call',
            _build_request_call,
        )
        result = load_mod._load_to_api_env(
            [{'id': 1}],
            {
                'url': 'https://example.test/api',
                'method': 'put',
                'headers': {'X-Test': '1'},
                'timeout': 2.0,
                'session': 'sess',
                'request_kwargs': {'verify': False},
            },
        )

        assert result['method'] == 'PUT'
        assert captured['url'] == 'https://example.test/api'
        assert captured['kwargs']['timeout'] == 9.5
        assert captured['kwargs']['headers'] == {'X-Test': '1'}
        assert captured['kwargs']['verify'] is False

    def test_load_to_api_env_falls_back_to_text_response_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that JSON decoding errors fall back to response text."""

        def _request(url: str, **kwargs: Any) -> JsonResponse:  # noqa: ARG001
            """Stub request function that returns a predefined response."""
            return JsonResponse(text='text payload', json_error=True)

        def _build_request_call(
            env: dict[str, Any],
            error_message: str,
            default_method: HttpMethod,
            json_data: object = None,
        ) -> SimpleNamespace:
            _ = error_message, default_method
            return SimpleNamespace(
                url=env['url'],
                request_callable=_request,
                timeout=10.0,
                http_method=HttpMethod.POST,
                kwargs={'json': json_data},
            )

        monkeypatch.setattr(
            load_mod,
            'build_request_call',
            _build_request_call,
        )
        result = load_mod._load_to_api_env(
            {'x': 1},
            {'url': 'https://example.test/api', 'method': 'post'},
        )

        assert result['response'] == 'text payload'


class TestLoadToDatabase:
    """Unit tests for :func:`etlplus.ops.load.load_to_database`."""

    def test_load_to_api_requires_callable(self) -> None:
        """
        Test that missing HTTP method on custom session raises
        :class:`TypeError`.
        """

        class _BrokenSession:
            pass

        with pytest.raises(TypeError):
            load_to_api(
                {'ok': True},
                'https://example.test/api',
                HttpMethod.POST,
                session=_BrokenSession(),
            )

    @pytest.mark.parametrize(
        ('field_name', 'expected'),
        [
            pytest.param('status', 'not_implemented', id='status'),
            pytest.param('records', 1, id='records'),
            pytest.param('connection_string', 'sqlite', id='connection-string'),
        ],
    )
    def test_load_to_database_returns_note(
        self,
        field_name: str,
        expected: object,
    ) -> None:
        """
        Test that placeholder implementation echoes the connection string.
        """

        data = [{'name': 'Ada'}]
        result = load_to_database(data, 'sqlite:///tmp.db')

        if field_name == 'connection_string':
            assert expected in result[field_name]
        else:
            assert result[field_name] == expected


class TestParseJsonString:
    """Unit tests for :func:`etlplus.ops.load._parse_json_string`."""

    def test_parse_invalid_root_raises(self) -> None:
        """Test that only dicts or lists of dicts are accepted."""

        with pytest.raises(
            ValueError,
            match='JSON root must be an object or array',
        ):
            _parse_json_string('"plain"')

    def test_parse_list_with_non_dicts_raises(self) -> None:
        """Test that mixed arrays raise :class:`ValueError`."""

        with pytest.raises(
            ValueError,
            match='JSON array must contain only objects',
        ):
            _parse_json_string('[{"ok": 1}, 3]')


class TestLoadApiOrchestrator:
    """
    Unit tests that ensure :func:`etlplus.ops.load.load` delegates to API
    loader.
    """

    @pytest.mark.parametrize(
        ('check_name', 'expected'),
        [
            pytest.param('status', 'success', id='status'),
            pytest.param('method', 'post', id='method'),
        ],
    )
    def test_load_api_with_default_method(
        self,
        check_name: str,
        expected: object,
    ) -> None:
        """
        Test that :func:`load` defaults to POST when the API method is omitted.
        """

        session = ApiSession()
        result = load(
            {'name': 'api'},
            DataConnectorType.API,
            'https://example.test/api',
            session=session,
        )

        result_dict = cast(dict[str, Any], result)
        (first_call,) = session.calls
        actual = result_dict['status'] if check_name == 'status' else first_call.method
        assert actual == expected

    def test_load_api_with_explicit_method(self) -> None:
        """Test that :func:`load` honors custom :class:`HttpMethod` values."""

        session = ApiSession()
        load(
            {'name': 'api'},
            DataConnectorType.API,
            'https://example.test/api',
            method=HttpMethod.PUT,
            session=session,
        )

        (first_call,) = session.calls
        assert first_call.method == 'put'

    def test_load_defensive_default_branch(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that unexpected connector coercion triggers :class:`ValueError`
        branch.
        """
        monkeypatch.setattr(
            load_mod.DataConnectorType,
            'coerce',
            classmethod(lambda cls, value: object()),
        )
        with pytest.raises(ValueError, match='Invalid target type'):
            load({'ok': True}, 'file', 'ignored')

    @pytest.mark.parametrize(
        ('check_name', 'expected'),
        [
            pytest.param('result', {'status': 'success'}, id='result'),
            pytest.param('call-count', 1, id='call-count'),
            pytest.param(
                'target',
                'https://example.com/files/data.csv?download=1',
                id='target',
            ),
            pytest.param(
                'options',
                {'encoding': 'utf-8', 'delimiter': ';'},
                id='options',
            ),
        ],
    )
    def test_load_file_dispatch_forwards_write_options(
        self,
        monkeypatch: pytest.MonkeyPatch,
        check_name: str,
        expected: object,
    ) -> None:
        """Test that file dispatch forwards kwargs as write options."""
        calls: list[tuple[Any, Any, Any, Any]] = []

        def _load_to_file(
            data: JSONData,
            target: str,
            file_format: object,
            options: object | None = None,
        ) -> JSONDict:
            calls.append((data, target, file_format, options))
            return {'status': 'success'}

        monkeypatch.setattr(load_mod, 'load_to_file', _load_to_file)

        result = load(
            {'ok': True},
            DataConnectorType.FILE,
            'https://example.com/files/data.csv?download=1',
            file_format='csv',
            encoding='utf-8',
            delimiter=';',
        )

        match check_name:
            case 'result':
                assert result == expected
            case 'call-count':
                assert len(calls) == expected
            case 'target':
                assert calls[0][1] == expected
            case 'options':
                assert calls[0][3] == expected
            case _:
                pytest.fail(f'unhandled check: {check_name}')


class TestLoadToFile:
    """
    Unit tests for :func:`etlplus.ops.load.load_to_file`.

    Notes
    -----
    - Tests writing to CSV and JSON files,
        directory creation, and error handling.
    """

    @pytest.mark.parametrize(
        ('check_name', 'expected'),
        [
            pytest.param('status', 'success', id='status'),
            pytest.param(
                'message',
                'Data loaded to s3://bucket/output.csv',
                id='message',
            ),
            pytest.param('path', 's3://bucket/output.csv', id='path'),
            pytest.param('file_format', load_mod.FileFormat.CSV, id='file-format'),
            pytest.param('encoding', 'utf-16', id='encoding'),
            pytest.param('extras', {'delimiter': '|'}, id='extras'),
        ],
    )
    def test_remote_uri_preserves_path_and_coerces_write_options(
        self,
        monkeypatch: pytest.MonkeyPatch,
        check_name: str,
        expected: object,
    ) -> None:
        """Test that remote file loads keep the URI and forward write options."""
        captured: dict[str, Any] = {}

        class _FakeFile:
            file_format = load_mod.FileFormat.CSV

            def __init__(
                self,
                path: str,
                file_format: object = None,
            ) -> None:
                captured['path'] = path
                captured['file_format'] = file_format

            def write(
                self,
                data: JSONData,
                *,
                options: object | None = None,
            ) -> int:
                captured['data'] = data
                captured['options'] = options
                return 1

        monkeypatch.setattr(load_mod, 'File', _FakeFile)

        result = load_to_file(
            [{'name': 'Ada'}],
            's3://bucket/output.csv',
            'csv',
            {'encoding': 'utf-16', 'delimiter': '|'},
        )

        options = captured['options']
        match check_name:
            case 'status' | 'message':
                assert result[check_name] == expected
            case 'path' | 'file_format':
                assert captured[check_name] == expected
            case 'encoding' | 'extras':
                assert options is not None
                assert getattr(options, check_name) == expected
            case _:
                pytest.fail(f'unhandled check: {check_name}')

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

    @pytest.mark.parametrize(
        ('check_name', 'expected'),
        [
            pytest.param('status', 'success', id='status'),
            pytest.param('records', 1, id='records'),
            pytest.param('exists', True, id='exists'),
            pytest.param('loaded-data', {'status': 'ok'}, id='loaded-data'),
        ],
    )
    def test_to_file_infers_format_when_none(
        self,
        tmp_path: Path,
        check_name: str,
        expected: object,
    ) -> None:
        """
        Test that omitting file_format infers from the output extension.
        """
        output_path = tmp_path / 'auto.json'
        payload = {'status': 'ok'}

        result = load_to_file(payload, str(output_path), None)

        match check_name:
            case 'status' | 'records':
                assert result[check_name] == expected
            case 'exists':
                assert output_path.exists() is expected
            case 'loaded-data':
                with open(output_path, encoding='utf-8') as f:
                    assert json.load(f) == expected
            case _:
                pytest.fail(f'unhandled check: {check_name}')

    @pytest.mark.parametrize(
        ('check_name', 'expected'),
        [
            pytest.param('status', 'success', id='status'),
            pytest.param('message', 'Data loaded to auto.json', id='message'),
            pytest.param('path', 'auto.json', id='path'),
            pytest.param('file_format', None, id='file-format'),
            pytest.param('root_tag', '99', id='root-tag'),
            pytest.param('table', '123', id='table'),
            pytest.param('dataset', '456', id='dataset'),
            pytest.param('inner_name', '789', id='inner-name'),
            pytest.param('extras', {'indent': 2}, id='extras'),
        ],
    )
    def test_to_file_infers_format_and_forwards_coerced_options_when_none(
        self,
        monkeypatch: pytest.MonkeyPatch,
        check_name: str,
        expected: object,
    ) -> None:
        """Test inferred format path when optional write options are supplied."""
        captured: dict[str, Any] = {}

        class _FakeFile:
            file_format = load_mod.FileFormat.JSON

            def __init__(
                self,
                path: str,
                file_format: object = None,
            ) -> None:
                captured['path'] = path
                captured['file_format'] = file_format

            def write(
                self,
                data: JSONData,
                *,
                options: object | None = None,
            ) -> int:
                captured['data'] = data
                captured['options'] = options
                return 1

        monkeypatch.setattr(load_mod, 'File', _FakeFile)

        result = load_to_file(
            {'status': 'ok'},
            'auto.json',
            None,
            {
                'root_tag': 99,
                'table': 123,
                'dataset': 456,
                'inner_name': 789,
                'indent': 2,
            },
        )

        options = captured['options']
        match check_name:
            case 'status' | 'message':
                assert result[check_name] == expected
            case 'path':
                assert captured['path'] == expected
            case 'file_format':
                assert captured['file_format'] is expected
            case 'root_tag' | 'table' | 'dataset' | 'inner_name' | 'extras':
                assert options is not None
                assert getattr(options, check_name) == expected
            case _:
                pytest.fail(f'unhandled check: {check_name}')

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
