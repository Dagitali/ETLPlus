"""
:mod:`tests.unit.file.test_u_file_toml` module.

Unit tests for :mod:`etlplus.file.toml`.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

import pytest

from etlplus.file import toml as mod

from .pytest_file_contract_mixins import OptionalModuleInstaller
from .pytest_file_contract_mixins import RoundtripSpec
from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_contracts import SemiStructuredReadModuleContract
from .pytest_file_contracts import SemiStructuredWriteDictModuleContract

# SECTION: HELPERS ========================================================== #


type DumpCall = dict[str, object]


@dataclass
class _TomlDumperStub:
    """Stub for TOML dumper modules exposing ``dumps``."""

    output: str
    calls: list[DumpCall] = field(default_factory=list)

    def dumps(
        self,
        payload: dict[str, object],
    ) -> str:
        """
        Simulate dumping by recording the payload and returning a fixed string.
        """
        self.calls.append(payload)
        return self.output


# SECTION: TESTS ============================================================ #


class TestToml(
    RoundtripUnitModuleContract,
    SemiStructuredReadModuleContract,
    SemiStructuredWriteDictModuleContract,
):
    """Unit tests for :mod:`etlplus.file.toml`."""

    _tomli_w_stub: _TomlDumperStub

    module = mod
    format_name = 'toml'
    sample_read_text = 'name = "etl"'
    expected_read_payload = {'name': 'etl'}
    dict_payload = {'name': 'etl'}
    roundtrip_spec = RoundtripSpec(
        payload={'name': 'etl'},
        expected={'name': 'etl'},
    )

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
        path = self.format_path(tmp_path, stem='config')
        path.write_text('name = "etl"', encoding='utf-8')
        monkeypatch.setattr(mod.tomllib, 'loads', lambda *_: ['bad'])

        with pytest.raises(TypeError, match='TOML root must be a table'):
            mod.TomlFile().read(path)

    def setup_roundtrip_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install ``tomli_w`` stub for roundtrip contract tests."""
        optional_module_stub({'tomli_w': _TomlDumperStub('name = "etl"\n')})

    def setup_write_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install ``tomli_w`` stub for write contract tests."""
        self._tomli_w_stub = _TomlDumperStub('tomli_w_output')
        optional_module_stub({'tomli_w': self._tomli_w_stub})

    def test_write_falls_back_to_toml_when_tomli_w_missing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`write` falls back to :func:`toml` when :func:`tomli_w`
        is missing.
        """
        stub = _TomlDumperStub('toml_output')

        def _get_optional_module(name: str, *, error_message: str) -> object:
            if name == 'tomli_w':
                raise ImportError(error_message)
            if name == 'toml':
                return stub
            raise AssertionError(f'Unexpected module {name!r}')

        monkeypatch.setattr(mod, 'get_optional_module', _get_optional_module)
        path = self.format_path(tmp_path, stem='config')

        written = mod.TomlFile().write(path, {'name': 'etl'})

        assert written == 1
        assert path.read_text(encoding='utf-8') == 'toml_output'
        assert stub.calls == [{'name': 'etl'}]
