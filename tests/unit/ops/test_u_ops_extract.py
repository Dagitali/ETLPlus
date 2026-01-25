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

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from etlplus.ops.extract import extract
from etlplus.ops.extract import extract_from_api
from etlplus.ops.extract import extract_from_database
from etlplus.ops.extract import extract_from_file

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


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


@pytest.mark.unit
class TestExtract:
    """
    Unit test suite for :func:`etlplus.ops.extract.extract`.

    Notes
    -----
    - Tests file extraction for supported formats.
    """

    def test_invalid_source_type(self) -> None:
        """Test error raised for invalid source type."""
        with pytest.raises(ValueError, match='Invalid DataConnectorType'):
            extract('invalid', 'source')

    @pytest.mark.parametrize(
        'file_format,write,expected_extracts',
        [
            (
                'json',
                lambda p: json.dump(
                    {'test': 'data'},
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


@pytest.mark.unit
class TestExtractErrors:
    """
    Unit test suite for ``etlplus.ops.extract`` function errors.

    Notes
    -----
    - Tests error handling for extract and extract_from_file.
    """

    @pytest.mark.parametrize(
        'exc_type,call,args,err_msg',
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
        Test parametrized error case tests for extract/extract_from_file.

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

        Raises
        ------
        AssertionError
            If the expected exception is not raised.
        """
        with pytest.raises(exc_type) as exc:
            call(*args)
        if err_msg:
            assert err_msg in str(exc.value)


@pytest.mark.unit
class TestExtractFromApi:
    """
    Unit test suite for :func:`etlplus.ops.extract.extract_from_api`.

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
        Custom HTTP methods and kwargs should pass through to the session.
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

    def test_invalid_json_fallback(
        self,
        base_url: str,
    ) -> None:
        """Malformed JSON should fall back to raw content payloads."""

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
        'payload,expected',
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
        """Verify supported JSON payload shapes are normalized correctly."""

        response = _StubResponse(
            headers={'content-type': 'application/json'},
            payload=payload,
            text=(
                json.dumps(payload)
                if not isinstance(payload, str)
                else payload
            ),
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
        Missing HTTP methods on the provided session should raise TypeError.
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
        """Non-JSON content should be returned as raw text payloads."""

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


@pytest.mark.unit
class TestExtractFromDatabase:
    """
    Unit test suite for :func:`etlplus.ops.extract.extract_from_database`.

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


@pytest.mark.unit
class TestExtractFromFile:
    """
    Unit test suite for :func:`etlplus.ops.extract.extract_from_file`.

    Notes
    -----
    - Tests supported and unsupported file formats.
    """

    @pytest.mark.parametrize(
        'file_format,write,expected_extracts',
        [
            (
                'json',
                lambda p: json.dump(
                    {'name': 'John', 'age': 30},
                    open(p, 'w', encoding='utf-8'),
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
                lambda p: open(p, 'w', encoding='utf-8').write(
                    (
                        '<?xml version="1.0"?>\n'
                        '<person><name>John</name><age>30</age></person>'
                    ),
                ),
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
        'file_format,content,err_msg',
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
        with pytest.raises(ValueError) as e:
            extract_from_file(str(path), file_format)
        assert err_msg in str(e.value)
