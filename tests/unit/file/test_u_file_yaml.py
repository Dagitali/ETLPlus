"""
:mod:`tests.unit.file.test_u_file_yaml` module.

Unit tests for :mod:`etlplus.file.yaml`.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any

import pytest

from etlplus.file import yaml as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions
from tests.unit.file.conftest import OptionalModuleInstaller
from tests.unit.file.conftest import RoundtripSpec
from tests.unit.file.conftest import RoundtripUnitModuleContract
from tests.unit.file.conftest import SemiStructuredReadModuleContract
from tests.unit.file.conftest import SemiStructuredWriteDictModuleContract

# SECTION: HELPERS ========================================================== #


type DumpCall = dict[str, object]


@dataclass
class _StubYaml:
    """Minimal PyYAML substitute."""

    loaded: object = field(default_factory=lambda: {'loaded': 'value'})
    load_calls: int = 0
    dump_calls: list[DumpCall] = field(default_factory=list)

    def safe_load(self, handle: Any) -> object:
        """Return configured loaded data."""
        self.load_calls += 1
        handle.read()
        return self.loaded

    def safe_dump(
        self,
        data: object,
        handle: Any,
        **kwargs: object,
    ) -> None:
        """Capture write calls and emit deterministic file content."""
        self.dump_calls.append({'data': data, 'kwargs': kwargs})
        handle.write('yaml')


# SECTION: TESTS ============================================================ #


class TestYaml(
    RoundtripUnitModuleContract,
    SemiStructuredReadModuleContract,
    SemiStructuredWriteDictModuleContract,
):
    """Unit tests for :mod:`etlplus.file.yaml`."""

    _read_yaml_stub: _StubYaml
    _write_yaml_stub: _StubYaml

    module = mod
    format_name = 'yaml'
    sample_read_text = 'name: etl\n'
    expected_read_payload = {'name': 'etl'}
    dict_payload = {'name': 'etl'}
    roundtrip_spec = RoundtripSpec(
        payload={'name': 'etl'},
        expected={'name': 'etl'},
    )

    def assert_read_contract_result(
        self,
        result: object,
    ) -> None:
        """Assert YAML read contract expectations."""
        assert result == self.expected_read_payload
        assert self._read_yaml_stub.load_calls == 1

    def assert_write_contract_result(
        self,
        path: Path,  # noqa: ARG002
    ) -> None:
        """Assert YAML write contract expectations."""
        assert self._write_yaml_stub.dump_calls == [
            {
                'data': self.dict_payload,
                'kwargs': {
                    'sort_keys': False,
                    'allow_unicode': True,
                    'default_flow_style': False,
                },
            },
        ]

    def setup_read_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install YAML dependency stub for read tests."""
        self._read_yaml_stub = _StubYaml(loaded=self.expected_read_payload)
        optional_module_stub({'yaml': self._read_yaml_stub})

    def setup_roundtrip_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install YAML stub for roundtrip contract tests."""
        optional_module_stub({'yaml': _StubYaml(loaded={'name': 'etl'})})

    def setup_write_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install YAML dependency stub for write tests."""
        self._write_yaml_stub = _StubYaml()
        optional_module_stub({'yaml': self._write_yaml_stub})

    def test_loads_rejects_scalar_yaml_root(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test YAML loads rejecting scalar roots."""
        optional_module_stub({'yaml': _StubYaml(loaded='scalar')})
        handler = mod.YamlFile()

        with pytest.raises(
            TypeError,
            match='YAML root must be an object or an array of objects',
        ):
            handler.loads('scalar')

    def test_read_honors_encoding_option(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test YAML reads honoring explicit text encoding options."""
        optional_module_stub({'yaml': _StubYaml(loaded={'name': 'José'})})
        path = self.format_path(tmp_path, stem='latin1')
        path.write_bytes('name: José\n'.encode('latin-1'))
        handler = mod.YamlFile()

        result = handler.read(path, options=ReadOptions(encoding='latin-1'))

        assert result == {'name': 'José'}

    def test_write_counts_records_for_list_payloads(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test YAML writes returning list record counts."""
        stub = _StubYaml()
        optional_module_stub({'yaml': stub})
        path = self.format_path(tmp_path, stem='list')
        handler = mod.YamlFile()

        written = handler.write(
            path,
            [{'id': 1}, {'id': 2}],
            options=WriteOptions(),
        )

        assert written == 2
        assert stub.dump_calls
