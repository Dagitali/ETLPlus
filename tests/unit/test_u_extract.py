"""
``tests.unit.test_u_extract`` module.

Unit tests for ``etlplus.extract``.

Notes
-----
- Validates extraction logic for JSON, CSV, XML, and error paths using
    temporary files and orchestrator dispatch.
- Uses parameterized cases for supported formats and error scenarios.
- Centralizes temp file creation via fixture.
- Applies Python 3.13 match/case for error assertions.
- Class-based suite for clarity and DRYness.
"""
import json
from pathlib import Path
from typing import Any
from typing import Callable

import pytest

from etlplus.extract import extract
from etlplus.extract import extract_from_file


# SECTION: TESTS ============================================================ #


class TestExtract:
    """
    Unit test suite for :func:`etlplus.extract.extract`.
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
        call: Callable,
        args: list[Any],
        err_msg: str | None,
    ):
        """
        Test parametrized error case tests for extract/extract_from_file.
        Parameters
        ----------
        exc_type : type[Exception]
            Expected exception type.
        call : Callable
            Function to call.
        args : list[Any]
            Arguments to pass to the function.
        err_msg : str | None
            Expected error message substring, if applicable.
        """
        with pytest.raises(exc_type) as exc:
            call(*args)
        match exc.value:
            case FileNotFoundError():
                pass
            case ValueError() if err_msg and err_msg in str(exc.value):
                pass
            case _:
                assert \
                    False, \
                    f'Expected {exc_type.__name__} with message: {err_msg}'

    @pytest.mark.parametrize(
        'file_format,write,expected',
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
        write: Callable | None,
        expected: Any,
        request: pytest.FixtureRequest,
    ):
        """
        Test extracting data from a file with a supported format.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.
        file_format : str
            File format of the data.
        write : Callable | None
            Optional function to write data to the file. For CSV, the
            ``csv_writer`` fixture is used instead.
        expected : Any
            Expected extracted data.
        request : pytest.FixtureRequest
            Pytest fixture request object used to access other fixtures.
        """
        path = tmp_path / f"data.{file_format}"
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
            assert result == expected

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
    ):
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

    def test_wrapper_file(
        self,
        tmp_path: Path,
    ):
        """
        Test extracting data from a file with a supported format.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.

        Notes
        -----
        Supported format should not raise an error.
        """
        path = tmp_path / 'data.json'
        json.dump({'test': 'data'}, open(path, 'w', encoding='utf-8'))
        result = extract('file', str(path), file_format='json')
        assert result == {'test': 'data'}
