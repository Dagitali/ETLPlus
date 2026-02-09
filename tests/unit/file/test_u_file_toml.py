"""
:mod:`tests.unit.file.test_u_file_toml` module.

Unit tests for :mod:`etlplus.file.toml`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import toml as mod
from tests.unit.file.conftest import SemiStructuredReadModuleContract
from tests.unit.file.conftest import SemiStructuredWriteDictModuleContract

# SECTION: HELPERS ========================================================== #


class _TomliWStub:
    """Stub for ``tomli_w`` module."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def dumps(
        self,
        payload: dict[str, object],
    ) -> str:
        """
        Simulate dumping by recording the payload and returning a fixed string.
        """
        self.calls.append(payload)
        return 'tomli_w_output'


class _TomlStub:
    """Stub for ``toml`` module."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def dumps(
        self,
        payload: dict[str, object],
    ) -> str:
        """
        Simulate dumping by recording the payload and returning a fixed string.
        """
        self.calls.append(payload)
        return 'toml_output'


# SECTION: TESTS ============================================================ #


class TestToml(
    SemiStructuredReadModuleContract,
    SemiStructuredWriteDictModuleContract,
):
    """Unit tests for :mod:`etlplus.file.toml`."""

    module = mod
    format_name = 'toml'
    sample_read_text = 'name = "etl"'
    expected_read_payload = {'name': 'etl'}
    dict_payload = {'name': 'etl'}

    def setup_write_dependencies(
        self,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Install ``tomli_w`` stub for write contract tests."""
        self._tomli_w_stub = _TomliWStub()
        optional_module_stub({'tomli_w': self._tomli_w_stub})

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert TOML write contract expectations."""
        assert path.read_text(encoding='utf-8') == 'tomli_w_output'
        assert self._tomli_w_stub.calls == [self.dict_payload]

    def test_read_non_table_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that reading a non-table TOML root raises :class:`TypeError`.
        """
        path = tmp_path / 'config.toml'
        path.write_text('name = "etl"', encoding='utf-8')
        monkeypatch.setattr(mod.tomllib, 'loads', lambda *_: ['bad'])

        with pytest.raises(TypeError, match='TOML root must be a table'):
            mod.read(path)

    def test_write_falls_back_to_toml_when_tomli_w_missing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`write` falls back to :func:`toml` when :func:`tomli_w`
        is missing.
        """
        stub = _TomlStub()

        def _get_optional_module(name: str, *, error_message: str) -> object:
            if name == 'tomli_w':
                raise ImportError(error_message)
            if name == 'toml':
                return stub
            raise AssertionError(f'Unexpected module {name!r}')

        monkeypatch.setattr(mod, 'get_optional_module', _get_optional_module)
        path = tmp_path / 'config.toml'

        written = mod.write(path, {'name': 'etl'})

        assert written == 1
        assert path.read_text(encoding='utf-8') == 'toml_output'
        assert stub.calls == [{'name': 'etl'}]
