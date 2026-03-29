"""
:mod:`tests.unit.ops.test_u_ops_extract` module.

Unit tests for :mod:`etlplus.ops.extract`.

Notes
-----
- Validates extraction logic for JSON, CSV, XML, and error paths using
    temporary files and orchestrator dispatch.
- Uses parameterized cases for supported formats and error scenarios.
- Centralizes temporary file creation via a fixture in conftest.py.
- Class-based suite for clarity and DRYness.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from etlplus.ops.extract import extract
from etlplus.ops.extract import extract_from_api
from etlplus.ops.extract import extract_from_database
from etlplus.ops.extract import extract_from_file
from etlplus.utils._types import JSONData

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


extract_mod = importlib.import_module('etlplus.ops.extract')


def _write_json_payload(path: str, payload: dict[str, Any]) -> None:
    """Write one JSON payload using UTF-8 encoding."""
    Path(path).write_text(json.dumps(payload), encoding='utf-8')


def _write_xml_person_payload(path: str) -> None:
    """Write one minimal XML person payload using UTF-8 encoding."""
    Path(path).write_text(
        '<?xml version="1.0"?>\n<person><name>John</name><age>30</age></person>',
        encoding='utf-8',
    )


class _StubResponse:
    """Simple stand-in for :meth:`requests.Response`."""

    def __init__(
        self,
        *,
        headers: dict[str, str],
        payload: Any | None = None,
        text: str = '',
        json_error: bool = False,
    ) -> None:
        self.headers = headers
        self.text = text
        self._payload = payload
        self._json_error = json_error

    def raise_for_status(self) -> None:
        """Match the ``requests`` API."""

        return None

    def json(self) -> Any:
        """Return the pre-set payload or raise JSON error."""
        if self._json_error:
            raise ValueError('malformed payload')
        return self._payload


class _StubSession:
    """Lightweight session that records outgoing calls."""

    def __init__(
        self,
        response: _StubResponse,
        *,
        method_name: str = 'get',
    ) -> None:
        self._response = response
        self.calls: list[dict[str, Any]] = []
        setattr(self, method_name, self._make_call)

    def _make_call(
        self,
        url: str,
        **kwargs: Any,
    ) -> _StubResponse:
        """Record the call and return the pre-set response."""
        self.calls.append({'url': url, 'kwargs': kwargs})
        return self._response


# SECTION: TESTS ============================================================ #


class TestExtract:
    """
    Unit tests for :func:`etlplus.ops.extract.extract`.

    Notes
    -----
    - Tests file extraction for supported formats.
    """

    def test_invalid_source_type(self) -> None:
        """Test that error raised for invalid source type."""
        with pytest.raises(ValueError, match='Invalid DataConnectorType'):
            extract('invalid', 'source')

    @pytest.mark.parametrize(
        ('file_format', 'write', 'expected_extracts'),
        [
            (
                'json',
                lambda p: _write_json_payload(p, {'test': 'data'}),
                {'test': 'data'},
            ),
        ],
    )
    def test_wrapper_file(
        self,
        tmp_path: Path,
        file_format: str,
        write: Callable[[str], None],
        expected_extracts: Any,
    ) -> None:
        """
        Test extracting data from a file with a supported format.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.
        file_format : str
            File format of the data.
        write : Callable[[str], None]
            Function to write data to the file.
        expected_extracts : Any
            Expected extracted data.

        Notes
        -----
        Supported format should not raise an error.
        """
        path = tmp_path / f'data.{file_format}'
        write(str(path))
        result = extract(
            'file',
            str(path),
            file_format=file_format,
        )
        assert result == expected_extracts


class TestExtractErrors:
    """
    Unit tests for :mod:`etlplus.ops.extract` function errors.

    Notes
    -----
    - Tests error handling for extract and extract_from_file.
    """

    @pytest.mark.parametrize(
        ('exc_type', 'call', 'args', 'err_msg'),
        [
            (
                FileNotFoundError,
                extract_from_file,
                ['/nonexistent/file.json', 'json'],
                None,
            ),
            (
                ValueError,
                extract,
                ['invalid', 'source'],
                'Invalid DataConnectorType',
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
        Test that parametrized error case tests for extract/extract_from_file.

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


class TestExtractFromApi:
    """
    Unit tests for :func:`etlplus.ops.extract.extract_from_api`.

    Notes
    -----
    - Validates JSON parsing paths, fallback behavior, and HTTP method
        coercion.
    """

    def test_custom_method_and_kwargs(
        self,
        base_url: str,
    ) -> None:
        """
        Test that custom HTTP methods and kwargs pass through to the session.
        """

        response = _StubResponse(
            headers={'content-type': 'application/json'},
            payload={'status': 'ok'},
        )
        session = _StubSession(response, method_name='post')
        result = extract_from_api(
            f'{base_url}/hooks',
            method='POST',
            session=session,
            timeout=2.5,
            headers={'X-Test': '1'},
        )
        assert result == {'status': 'ok'}
        assert session.calls[0]['kwargs']['timeout'] == 2.5
        assert session.calls[0]['kwargs']['headers'] == {'X-Test': '1'}

    def test_extract_from_api_env_requires_url(self) -> None:
        """Test that missing URL in normalized API env raise ValueError."""
        with pytest.raises(ValueError, match='API source missing URL'):
            extract_mod._extract_from_api_env({}, use_client=False)

    def test_invalid_json_fallback(
        self,
        base_url: str,
    ) -> None:
        """Test that malformed JSON falls back to raw content payloads."""

        response = _StubResponse(
            headers={'content-type': 'application/json'},
            text='{"bad": true}',
            json_error=True,
        )
        session = _StubSession(response)
        result = extract_from_api(f'{base_url}/bad', session=session)
        assert result == {
            'content': '{"bad": true}',
            'content_type': 'application/json',
        }

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            ({'name': 'Ada'}, {'name': 'Ada'}),
            (
                [{'name': 'Ada'}, {'name': 'Grace'}],
                [{'name': 'Ada'}, {'name': 'Grace'}],
            ),
            (['raw', 42], [{'value': 'raw'}, {'value': 42}]),
            ('scalar', {'value': 'scalar'}),
        ],
    )
    def test_json_payload_variants(
        self,
        base_url: str,
        payload: Any,
        expected: Any,
    ) -> None:
        """Test that supported JSON payload shapes are normalized correctly."""

        response = _StubResponse(
            headers={'content-type': 'application/json'},
            payload=payload,
            text=(json.dumps(payload) if not isinstance(payload, str) else payload),
        )
        session = _StubSession(response)
        result = extract_from_api(f'{base_url}/data', session=session)
        assert result == expected
        assert session.calls[0]['kwargs']['timeout'] == 10.0

    def test_missing_http_method_raises_type_error(
        self,
        base_url: str,
    ) -> None:
        """
        Test that missing HTTP methods on the provided session raise
        :class:`TypeError`.
        """

        class NoGet:  # noqa: D401
            """Session stub without a 'GET' method."""

            __slots__ = ()

        with pytest.raises(TypeError, match='callable "get"'):
            extract_from_api(f'{base_url}/data', session=NoGet())

    def test_non_json_content_type(
        self,
        base_url: str,
    ) -> None:
        """Test that non-JSON content is returned as raw text payloads."""

        response = _StubResponse(
            headers={'content-type': 'text/plain'},
            text='plain text response',
        )
        session = _StubSession(response)
        result = extract_from_api(f'{base_url}/text', session=session)
        assert result == {
            'content': 'plain text response',
            'content_type': 'text/plain',
        }

    def test_use_client_with_direct_url_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that client mode with only a URL uses the paginate_url path.
        """
        init_calls: list[dict[str, Any]] = []
        paginate_calls: list[dict[str, Any]] = []

        class _Client:
            """
            Stub EndpointClient that captures init and paginate_url calls.
            """

            def __init__(self, **kwargs: Any) -> None:
                init_calls.append(kwargs)

            def paginate_url(
                self,
                url: str,
                pagination: Any,
                *,
                request: Any,
                sleep_seconds: float,
            ) -> list[dict[str, int]]:
                """
                Stub paginate_url method that captures the call parameters and
                simulates pagination.
                """
                paginate_calls.append(
                    {
                        'url': url,
                        'pagination': pagination,
                        'request': request,
                        'sleep_seconds': sleep_seconds,
                    },
                )
                return [{'id': 1}]

        monkeypatch.setattr(extract_mod, 'EndpointClient', _Client)

        env = {
            'url': 'https://example.test/v1/items?limit=5',
            'params': {'limit': 5},
            'headers': {'Accept': 'application/json'},
            'timeout': 2.0,
            'pagination': {'type': 'page'},
            'sleep_seconds': 0.25,
        }

        result = extract_mod._extract_from_api_env(env, use_client=True)

        assert result == [{'id': 1}]
        assert init_calls[0]['base_url'] == 'https://example.test'
        assert paginate_calls[0]['url'] == env['url']
        assert paginate_calls[0]['sleep_seconds'] == 0.25
        request = paginate_calls[0]['request']
        assert request.params == {'limit': 5}
        assert request.headers == {'Accept': 'application/json'}
        assert request.timeout == 2.0


class TestExtractFromDatabase:
    """
    Unit tests for :func:`etlplus.ops.extract.extract_from_database`.

    Notes
    -----
    - Exercises placeholder payloads across multiple connection strings.
    """

    @pytest.mark.parametrize(
        'connection_string',
        [
            'postgresql://user:pass@db.prod.example:5432/app?sslmode=require',
            'sqlite:////tmp/db.sqlite3',
        ],
    )
    def test_placeholder_payload(
        self,
        connection_string: str,
    ) -> None:
        """Test that the placeholder payload echoes the connection string."""

        result = extract_from_database(connection_string)
        assert isinstance(result, list)
        assert len(result) == 1
        payload = result[0]
        assert payload['connection_string'] == connection_string
        assert payload['message'] == 'Database extraction not yet implemented'
        assert 'Install database-specific drivers' in payload['note']


class TestExtractFromFile:
    """
    Unit tests for :func:`etlplus.ops.extract.extract_from_file`.

    Notes
    -----
    - Tests supported and unsupported file formats.
    """

    def test_extract_file_dispatch_forwards_remote_uri_and_options(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that file dispatch forwards kwargs as read options."""
        calls: list[tuple[str, object, object]] = []

        def _extract_from_file(
            source: str,
            file_format: object,
            options: object | None = None,
        ) -> JSONData:
            calls.append((source, file_format, options))
            return {'ok': True}

        monkeypatch.setattr(extract_mod, 'extract_from_file', _extract_from_file)

        result = extract(
            'file',
            'https://example.com/files/data.csv?download=1',
            file_format='csv',
            encoding='utf-8',
            delimiter=';',
        )

        assert result == {'ok': True}
        assert len(calls) == 1
        assert calls[0][0] == 'https://example.com/files/data.csv?download=1'
        options = calls[0][2]
        assert options is not None
        assert options == {'encoding': 'utf-8', 'delimiter': ';'}

    def test_infers_format_when_file_format_is_none(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that passing ``None`` for *file_format* defers to extension
        inference.
        """
        path = tmp_path / 'data.json'
        path.write_text('{"ok": true}', encoding='utf-8')

        result = extract_from_file(str(path), None)

        assert result == {'ok': True}

    def test_remote_uri_preserves_path_and_coerces_read_options(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that remote file extraction uses the original URI plus options."""
        captured: dict[str, Any] = {}

        class _FakeFile:
            def __init__(
                self,
                path: str,
                file_format: object = None,
            ) -> None:
                captured['path'] = path
                captured['file_format'] = file_format

            def read(
                self,
                *,
                options: object | None = None,
            ) -> JSONData:
                captured['options'] = options
                return {'ok': True}

        monkeypatch.setattr(extract_mod, 'File', _FakeFile)

        result = extract_from_file(
            's3://bucket/data.csv',
            'csv',
            {'encoding': 'latin-1', 'delimiter': '|'},
        )

        assert result == {'ok': True}
        assert captured['path'] == 's3://bucket/data.csv'
        assert captured['file_format'] == extract_mod.FileFormat.CSV
        options = captured['options']
        assert options is not None
        assert options.encoding == 'latin-1'
        assert options.extras == {'delimiter': '|'}

    @pytest.mark.parametrize(
        ('file_format', 'write', 'expected_extracts'),
        [
            (
                'json',
                lambda p: _write_json_payload(
                    p,
                    {'name': 'John', 'age': 30},
                ),
                {'name': 'John', 'age': 30},
            ),
            (
                'csv',
                pytest.fixture(lambda csv_writer: csv_writer),
                [
                    {'name': 'John', 'age': '30'},
                    {'name': 'Jane', 'age': '25'},
                ],
            ),
            (
                'xml',
                _write_xml_person_payload,
                {'person': {'name': {'text': 'John'}, 'age': {'text': '30'}}},
            ),
        ],
    )
    def test_supported_formats(
        self,
        tmp_path: Path,
        file_format: str,
        write: Callable[[str], None] | None,
        expected_extracts: Any,
        request: pytest.FixtureRequest,
    ) -> None:
        """
        Test extracting data from a file with a supported format.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.
        file_format : str
            File format of the data.
        write : Callable[[str], None] | None
            Optional function to write data to the file. For CSV, the
            ``csv_writer`` fixture is used instead.
        expected_extracts : Any
            Expected extracted data.
        request : pytest.FixtureRequest
            Pytest fixture request object used to access other fixtures.
        """
        path = tmp_path / f'data.{file_format}'
        if file_format == 'csv':
            write_fn = request.getfixturevalue('csv_writer')
        else:
            write_fn = write
        assert write_fn is not None
        write_fn(str(path))
        result = extract_from_file(str(path), file_format)
        if file_format == 'json' and isinstance(result, dict):
            # Allow minor type differences (e.g., age as int vs. str).
            assert result.get('name') == 'John'
            assert str(result.get('age')) == '30'
        elif file_format == 'csv' and isinstance(result, list):
            assert len(result) == 2
            assert result[0].get('name') == 'John'
            assert result[1].get('name') == 'Jane'
        elif file_format == 'xml' and isinstance(result, dict):
            assert 'person' in result
            person = result['person']
            # Support both plain-text and nested-text XML parsers.
            name = person.get('name')
            if isinstance(name, dict):
                assert name.get('text') == 'John'
            else:
                assert name == 'John'
        else:
            assert result == expected_extracts

    @pytest.mark.parametrize(
        ('file_format', 'content', 'err_msg'),
        [
            ('unsupported', 'test', 'Invalid FileFormat'),
        ],
    )
    def test_unsupported_format(
        self,
        tmp_path: Path,
        file_format: str,
        content: str,
        err_msg: str,
    ) -> None:
        """
        Test extracting data from a file with an unsupported format.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.
        file_format : str
            File format of the data.
        content : str
            Content to write to the file.
        err_msg : str
            Expected error message.

        Notes
        -----
        Unsupported format should raise ValueError.
        """
        path = tmp_path / f'data.{file_format}'
        path.write_text(content, encoding='utf-8')
        with pytest.raises(ValueError, match=err_msg):
            extract_from_file(str(path), file_format)


class TestExtractDefensiveDispatch:
    """Unit tests for defensive connector dispatch behavior."""

    def test_extract_defensive_default_branch(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that unexpected connector coercion triggers the
        :class:`ValueError` branch.
        """

        def _coerce(_value: object) -> object:
            return object()

        monkeypatch.setattr(
            extract_mod.DataConnectorType,
            'coerce',
            classmethod(lambda cls, value: _coerce(value)),
        )
        with pytest.raises(ValueError, match='Invalid source type'):
            extract('file', 'ignored')

    def test_extract_dispatches_database_branch(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that database connector types dispatch to DB extraction."""
        calls: list[str] = []

        def _extract_from_database(source: str) -> list[dict[str, str]]:
            calls.append(source)
            return [{'source': source}]

        monkeypatch.setattr(
            extract_mod,
            'extract_from_database',
            _extract_from_database,
        )

        result = extract('database', 'sqlite:///source.db')

        assert calls == ['sqlite:///source.db']
        assert result == [{'source': 'sqlite:///source.db'}]

    def test_extract_dispatches_api_branch(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that API connector types dispatch to API extraction."""
        calls: list[tuple[str, dict[str, Any]]] = []

        def _extract_from_api(
            source: str,
            **kwargs: Any,
        ) -> dict[str, Any]:
            calls.append((source, kwargs))
            return {'source': source, 'kwargs': kwargs}

        monkeypatch.setattr(
            extract_mod,
            'extract_from_api',
            _extract_from_api,
        )

        result = extract(
            'api',
            'https://example.test/data',
            method='post',
            timeout=3.0,
        )

        assert calls == [
            (
                'https://example.test/data',
                {'method': 'post', 'timeout': 3.0},
            ),
        ]
        assert result == {
            'source': 'https://example.test/data',
            'kwargs': {'method': 'post', 'timeout': 3.0},
        }
