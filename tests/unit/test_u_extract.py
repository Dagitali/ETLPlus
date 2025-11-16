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
import csv
import json
from pathlib import Path
from typing import Any
from typing import Callable

import pytest

from etlplus.extract import extract
from etlplus.extract import extract_from_file


# SECTION: HELPERS ========================================================== #


def _write_csv(
    path: str,
) -> None:
    """
    Helper function to write a CSV file with sample data.

    Parameters
    ----------
    path : str
        Path to the CSV file to write.
    """
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['name', 'age'])
        writer.writeheader()
        writer.writerows(
            [
                {'name': 'John', 'age': '30'},
                {'name': 'Jane', 'age': '25'},
            ],
        )


# SECTION: TESTS ============================================================ #


class TestExtract:
    """
    Unit test suite for the :func:`etlplus.extract.extract` function.
    """

    def test_file_not_found(self):
        """
        Test extracting data from a non-existent file.

        Notes
        -----
        Should raise FileNotFoundError.
        """
        with pytest.raises(FileNotFoundError) as exc:
            extract_from_file('/nonexistent/file.json', 'json')
        match exc.value:
            case FileNotFoundError():
                pass
            case _:
                assert False, 'Expected FileNotFoundError'

    def test_invalid_source_type(self):
        """
        Test extracting data with an invalid source type.

        Notes
        -----
        Invalid source type should raise ValueError.
        """
        with pytest.raises(ValueError) as e:
            extract('invalid', 'source')
        if 'Invalid DataConnectorType' in str(e.value):
            pass
        else:
            assert False, 'Expected ValueError for invalid source type'

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
                _write_csv,
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
        write: Callable,
        expected: Any,
    ):
        """
        Test extracting data from a file with a supported format.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.
        file_format : str
            File format of the data.
        write : Callable
            Function to write data to the file.
        expected : Any
            Expected extracted data.
        """
        path = tmp_path / f"data.{file_format}"
        write(str(path))
        result = extract_from_file(str(path), file_format)
        if file_format == 'csv' and isinstance(result, list):
            assert len(result) == 2
            assert result[0]['name'] == 'John'
            assert result[1]['name'] == 'Jane'
        elif file_format == 'xml' and isinstance(result, dict):
            assert 'person' in result
            assert result['person']['name']['text'] == 'John'
        else:
            assert result == expected

    def test_unsupported_format(
        self,
        tmp_path: Path,
    ):
        """
        Test extracting data from a file with an unsupported format.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory provided by pytest.

        Notes
        -----
        Unsupported format should raise ValueError.
        """
        path = tmp_path / 'data.txt'
        path.write_text('test', encoding='utf-8')
        with pytest.raises(ValueError) as exc:
            extract_from_file(str(path), 'unsupported')
        if 'Invalid FileFormat' in str(exc.value):
            pass
        else:
            assert False, 'Expected ValueError for invalid format'

    def test_wrapper_file(self, tmp_path):
        """
        Test extracting data from a file with a supported format.

        Notes
        -----
        Supported format should not raise an error.
        """
        path = tmp_path / 'data.json'
        json.dump({'test': 'data'}, open(path, 'w', encoding='utf-8'))
        result = extract('file', str(path), file_format='json')
        assert result == {'test': 'data'}
