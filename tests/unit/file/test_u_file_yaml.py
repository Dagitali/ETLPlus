"""
:mod:`tests.unit.file.test_u_file_yaml` module.

Unit tests for :mod:`etlplus.file.yaml`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from etlplus.file import yaml as mod

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


class TestYamlRead:
    """Unit tests for :func:`etlplus.file.yaml.read`."""

    def test_read_uses_yaml_module(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`read` uses the :mod:`yaml` module when available.
        """
        path = tmp_path / 'data.yaml'
        path.write_text('name: etl\n', encoding='utf-8')
        yaml_stub = _StubYaml({'name': 'etl'})
        optional_module_stub({'yaml': yaml_stub})

        result = mod.read(path)

        assert result == {'name': 'etl'}
        assert yaml_stub.load_calls == 1


class TestYamlWrite:
    """Unit tests for :func:`etlplus.file.yaml.write`."""

    def test_write_uses_yaml_module_and_options(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`write` uses the :mod:`yaml` module when available.
        """
        path = tmp_path / 'data.yaml'
        payload = [{'name': 'etl'}]
        yaml_stub = _StubYaml()
        optional_module_stub({'yaml': yaml_stub})

        written = mod.write(path, payload)

        assert written == 1
        assert yaml_stub.dump_calls == [
            {
                'data': payload,
                'kwargs': {
                    'sort_keys': False,
                    'allow_unicode': True,
                    'default_flow_style': False,
                },
            },
        ]
