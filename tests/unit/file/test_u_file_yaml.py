"""
:mod:`tests.unit.test_u_file_yaml` module.

Unit tests for :mod:`etlplus.file.yaml`.

Notes
-----
- Uses ``tmp_path`` for filesystem isolation.
- Exercises JSON detection and defers errors for unknown extensions.
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest

import etlplus.file._imports as import_helpers
from etlplus.file import File
from etlplus.file import FileFormat

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


_YAML_CACHE = import_helpers._MODULE_CACHE  # pylint: disable=protected-access


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
    stub = _StubYaml()
    _YAML_CACHE.clear()
    _YAML_CACHE['yaml'] = stub
    yield stub
    _YAML_CACHE.clear()


# SECTION: TESTS ============================================================ #


class TestYamlSupport:
    """Unit tests exercising YAML read/write helpers using a PyYAML stub."""

    def test_read_yaml_uses_stub(
        self,
        tmp_path: Path,
        yaml_stub: _StubYaml,
    ) -> None:
        """
        Test reading YAML should invoke stub ``safe_load``.
        """
        assert _YAML_CACHE['yaml'] is yaml_stub
        path = tmp_path / 'data.yaml'
        path.write_text('name: etl', encoding='utf-8')

        result = File(path, FileFormat.YAML).read()

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

        written = File(path, FileFormat.YAML).write(payload)

        assert written == 1
        assert yaml_stub.dump_calls
        assert yaml_stub.dump_calls[0]['data'] == payload
