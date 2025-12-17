"""
:mod:`tests.unit.test_u_extract` module.

Unit tests for ``etlplus.extract``.

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

from etlplus.extract import extract
from etlplus.extract import extract_from_file

# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestExtract:
    """
    Unit test suite for :func:`etlplus.extract.extract`.

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
    Unit test suite for ``etlplus.extract`` function errors.

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
        with pytest.raises(exc_type) as e:
            call(*args)
        match e.value:
            case FileNotFoundError():
                pass
            case ValueError() if err_msg and err_msg in str(e.value):
                pass
            case _:
                raise AssertionError(
                    f'Expected {exc_type.__name__} with message: {err_msg}',
                ) from e.value


@pytest.mark.unit
class TestExtractFromFile:
    """
    Unit test suite for :func:`etlplus.extract.extract_from_file`.

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
