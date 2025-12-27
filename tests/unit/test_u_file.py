"""
:mod:`tests.unit.test_u_file` module.

Unit tests for :mod:`etlplus.file`.

Notes
-----
- Uses ``tmp_path`` for filesystem isolation.
- Exercises JSON detection and defers errors for unknown extensions.
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import cast

import pytest

import etlplus.file as file_module
from etlplus.enums import FileFormat
from etlplus.file import File

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


class _StubYaml:
    """Minimal PyYAML substitute to avoid optional dependency in tests."""

    def __init__(self) -> None:
        self.dump_calls: list[dict[str, object]] = []

    def safe_load(
        self,
        handle: object,
    ) -> dict[str, str]:
        """Stub for PyYAML's ``safe_load`` function."""
        text = ''
        if hasattr(handle, 'read'):  # type: ignore[call-arg]
            text = handle.read()
        return {'loaded': str(text).strip()}

    def safe_dump(
        self,
        data: object,
        handle: object,
        **kwargs: object,
    ) -> None:
        """Stub for PyYAML's ``safe_dump`` function."""
        self.dump_calls.append({'data': data, 'kwargs': kwargs})
        if hasattr(handle, 'write'):
            handle.write('yaml')  # type: ignore[call-arg]


@pytest.fixture(name='yaml_stub')
def yaml_stub_fixture() -> Generator[_StubYaml]:
    """Install a stub PyYAML module for YAML tests."""
    # pylint: disable=protected-access

    stub = _StubYaml()
    file_module._YAML_CACHE.clear()
    file_module._YAML_CACHE['mod'] = stub
    yield stub
    file_module._YAML_CACHE.clear()


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestFile:
    """
    Unit test suite for :class:`etlplus.file.File`.

    Notes
    -----
    - Exercises JSON detection and defers errors for unknown extensions.
    """

    def test_classmethods_delegate(
        self,
        tmp_path: Path,
    ) -> None:
        """
        ``read_file`` and ``write_file`` should round-trip via classmethods.
        """

        path = tmp_path / 'delegated.json'
        data = {'name': 'delegated'}

        File.write_file(path, data, file_format='json')
        result = File.read_file(path, file_format='json')

        assert isinstance(result, dict)
        assert result['name'] == 'delegated'

    @pytest.mark.parametrize(
        'filename,expected_format,expected_content',
        [
            ('data.json', FileFormat.JSON, {}),
        ],
    )
    def test_infers_json_from_extension(
        self,
        tmp_path: Path,
        filename: str,
        expected_format: FileFormat,
        expected_content: dict[str, object],
    ) -> None:
        """
        Test JSON file inference from extension.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory path.
        filename : str
            Name of the file to create.
        expected_format : FileFormat
            Expected file format.
        expected_content : dict[str, object]
            Expected content after reading the file.
        """
        p = tmp_path / filename
        p.write_text('{}', encoding='utf-8')
        f = File(p)
        assert f.file_format == expected_format
        assert f.read() == expected_content

    def test_read_csv_skips_blank_rows(
        self,
        tmp_path: Path,
    ) -> None:
        """Test CSV reader ignoring empty rows."""

        payload = 'name,age\nJohn,30\n,\nJane,25\n'
        path = tmp_path / 'data.csv'
        path.write_text(payload, encoding='utf-8')

        rows = File(path, FileFormat.CSV).read_csv()

        assert [row['name'] for row in rows] == ['John', 'Jane']

    def test_read_json_type_errors(self, tmp_path: Path) -> None:
        """Test list elements being dicts when reading JSON."""

        path = tmp_path / 'bad.json'
        path.write_text('[{"ok": 1}, 2]', encoding='utf-8')

        with pytest.raises(TypeError):
            File(path, FileFormat.JSON).read_json()

    @pytest.mark.parametrize(
        'filename,expected_format',
        [
            ('weird.data', None),
        ],
    )
    def test_unknown_extension_defers_error(
        self,
        tmp_path: Path,
        filename: str,
        expected_format: FileFormat | None,
    ) -> None:
        """
        Test unknown file extension handling and error deferral.

        Ensures :class:`FileFormat` is None and reading raises
        :class:`ValueError`.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory path.
        filename : str
            Name of the file to create.
        expected_format : FileFormat | None
            Expected file format (should be None).
        """
        p = tmp_path / filename
        p.write_text('{}', encoding='utf-8')
        f = File(p)
        assert f.file_format is expected_format
        with pytest.raises(ValueError) as e:
            f.read()
        assert 'Cannot infer file format' in str(e.value)

    def test_write_csv_filters_non_dicts(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test non-dict entries being ignored when writing CSV rows.
        """

        path = tmp_path / 'data.csv'
        invalid_entry = cast(dict[str, object], 'invalid')
        count = File(path, FileFormat.CSV).write_csv(
            [{'name': 'John'}, invalid_entry],
        )

        assert count == 1
        assert 'name' in path.read_text(encoding='utf-8')

    def test_write_json_returns_record_count(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test ``write_json`` returning the record count for lists.
        """

        path = tmp_path / 'data.json'
        records = [{'a': 1}, {'a': 2}]

        written = File(path, FileFormat.JSON).write_json(records)

        assert written == 2
        json_content = path.read_text(encoding='utf-8')
        assert json_content
        assert json_content.count('\n') >= 2

    def test_xml_round_trip(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test XML write/read preserving nested dictionaries.
        """

        path = tmp_path / 'data.xml'
        payload = {'root': {'items': [{'text': 'one'}, {'text': 'two'}]}}

        File(path, FileFormat.XML).write_xml(payload)
        result = File(path, FileFormat.XML).read_xml()

        assert result['root']['items'][0]['text'] == 'one'

    def test_xml_respects_root_tag(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test custom root_tag being used when data lacks a single root.
        """

        path = tmp_path / 'export.xml'
        records = [{'name': 'Ada'}, {'name': 'Linus'}]

        File(path, FileFormat.XML).write_xml(records, root_tag='records')

        text = path.read_text(encoding='utf-8')
        assert text.startswith('<?xml')
        assert '<records>' in text


@pytest.mark.unit
class TestYamlSupport:
    """Unit tests exercising YAML read/write helpers without PyYAML."""

    def test_read_yaml_uses_stub(
        self,
        tmp_path: Path,
        yaml_stub: _StubYaml,
    ) -> None:
        """
        Test reading YAML should invoke stub ``safe_load``.
        """

        path = tmp_path / 'data.yaml'
        path.write_text('name: etl', encoding='utf-8')

        result = File(path, FileFormat.YAML).read_yaml()

        assert result == {'loaded': 'name: etl'}

    def test_write_yaml_uses_stub(
        self,
        tmp_path: Path,
        yaml_stub: _StubYaml,
    ) -> None:
        """
        Test writing YAML should invoke stub ``safe_dump``.
        """

        path = tmp_path / 'data.yaml'
        payload = [{'name': 'etl'}]

        written = File(path, FileFormat.YAML).write_yaml(payload)

        assert written == 1
        assert yaml_stub.dump_calls
        assert yaml_stub.dump_calls[0]['data'] == payload
