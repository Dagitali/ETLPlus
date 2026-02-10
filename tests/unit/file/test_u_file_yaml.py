"""
:mod:`tests.unit.file.test_u_file_yaml` module.

Unit tests for :mod:`etlplus.file.yaml`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from etlplus.file import yaml as mod
from tests.unit.file.conftest import SemiStructuredReadModuleContract
from tests.unit.file.conftest import SemiStructuredWriteDictModuleContract

# SECTION: HELPERS ========================================================== #


class _StubYaml:
    """Minimal PyYAML substitute."""

    def __init__(self, loaded: object | None = None) -> None:
        self._loaded = loaded if loaded is not None else {'loaded': 'value'}
        self.load_calls = 0
        self.dump_calls: list[dict[str, object]] = []

    def safe_load(self, handle: Any) -> object:
        """Return configured loaded data."""
        self.load_calls += 1
        _ = handle.read()
        return self._loaded

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
    SemiStructuredReadModuleContract,
    SemiStructuredWriteDictModuleContract,
):
    """Unit tests for :mod:`etlplus.file.yaml`."""

    module = mod
    format_name = 'yaml'
    sample_read_text = 'name: etl\n'
    expected_read_payload = {'name': 'etl'}
    dict_payload = {'name': 'etl'}

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
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Install YAML dependency stub for read tests."""
        self._read_yaml_stub = _StubYaml(self.expected_read_payload)
        optional_module_stub({'yaml': self._read_yaml_stub})

    def setup_write_dependencies(
        self,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Install YAML dependency stub for write tests."""
        self._write_yaml_stub = _StubYaml()
        optional_module_stub({'yaml': self._write_yaml_stub})
