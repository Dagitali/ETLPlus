"""
:mod:`tests.unit.test_u_cli_handlers` module.

Unit tests for :mod:`etlplus.cli.handlers`.
"""

from __future__ import annotations

import argparse
import types
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Final
from typing import cast
from unittest.mock import Mock

import pytest

import etlplus.cli.handlers as handlers
from etlplus.config import PipelineConfig

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

CSV_TEXT: Final[str] = 'a,b\n1,2\n3,4\n'


@dataclass(frozen=True, slots=True)
class DummyCfg:
    """Minimal stand-in pipeline config for CLI helper tests."""

    name: str = 'p1'
    version: str = 'v1'
    sources: list[object] = field(
        default_factory=lambda: [types.SimpleNamespace(name='s1')],
    )
    targets: list[object] = field(
        default_factory=lambda: [types.SimpleNamespace(name='t1')],
    )
    transforms: list[object] = field(
        default_factory=lambda: [types.SimpleNamespace(name='tr1')],
    )
    jobs: list[object] = field(
        default_factory=lambda: [types.SimpleNamespace(name='j1')],
    )


# SECTION: TESTS ============================================================ #


class TestCliHandlersInternalHelpers:
    """Unit tests for internal CLI helpers in :mod:`etlplus.cli.handlers`."""

    @pytest.mark.parametrize(
        ('behavior', 'expected_err', 'should_raise'),
        [
            pytest.param('ignore', '', False, id='ignore'),
            pytest.param('silent', '', False, id='silent'),
            pytest.param('warn', 'Warning:', False, id='warn'),
            pytest.param('error', '', True, id='error'),
        ],
    )
    def test_emit_behavioral_notice(
        self,
        behavior: str,
        expected_err: str,
        should_raise: bool,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """
        Test that behavioral notice raises or emits stderr per configured
        behavior.
        """
        # pylint: disable=protected-access

        if should_raise:
            with pytest.raises(ValueError):
                handlers._emit_behavioral_notice('msg', behavior, quiet=False)
            return

        handlers._emit_behavioral_notice('msg', behavior, quiet=False)
        captured = capsys.readouterr()
        assert expected_err in captured.err

    def test_emit_behavioral_notice_quiet_suppresses(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that quiet mode suppresses warning emission."""
        # pylint: disable=protected-access

        handlers._emit_behavioral_notice('msg', 'warn', quiet=True)
        captured = capsys.readouterr()
        assert captured.err == ''

    def test_format_behavior_strict(self) -> None:
        """Test that strict mode maps to error behavior."""
        # pylint: disable=protected-access

        assert handlers._format_behavior(True) == 'error'

    def test_format_behavior_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that the environment overrides behavior when not strict."""
        # pylint: disable=protected-access

        monkeypatch.setenv(handlers.FORMAT_ENV_KEY, 'fail')
        assert handlers._format_behavior(False) == 'fail'

        monkeypatch.delenv(handlers.FORMAT_ENV_KEY, raising=False)
        assert handlers._format_behavior(False) == 'warn'

    @pytest.mark.parametrize(
        ('resource_type', 'format_explicit', 'should_raise'),
        [
            pytest.param('file', True, True, id='file-explicit'),
            pytest.param('file', False, False, id='file-implicit'),
            pytest.param('database', True, False, id='nonfile-explicit'),
        ],
    )
    def test_handle_format_guard(
        self,
        monkeypatch: pytest.MonkeyPatch,
        resource_type: str,
        format_explicit: bool,
        should_raise: bool,
    ) -> None:
        """
        Test that the guard raises only for explicit formats on file resources.
        """
        # pylint: disable=protected-access

        monkeypatch.setattr(
            handlers,
            '_format_behavior',
            lambda _strict: 'error',
        )

        def call() -> None:
            handlers._handle_format_guard(
                io_context='source',
                resource_type=resource_type,
                format_explicit=format_explicit,
                strict=False,
                quiet=False,
            )

        if should_raise:
            with pytest.raises(ValueError):
                call()
        else:
            call()

    def test_list_sections_all(self) -> None:
        """
        Test that :func:`_list_sections` includes all requested sections."""
        # pylint: disable=protected-access

        args = argparse.Namespace(
            pipelines=True,
            sources=True,
            targets=True,
            transforms=True,
        )
        cfg = cast(PipelineConfig, DummyCfg())
        result = handlers._list_sections(
            cfg,
            args,
        )
        assert set(result) >= {'pipelines', 'sources', 'targets', 'transforms'}

    def test_list_sections_default(self) -> None:
        """`_list_sections` defaults to jobs when no flags are set."""
        # pylint: disable=protected-access

        args = argparse.Namespace(
            pipelines=False,
            sources=False,
            targets=False,
            transforms=False,
        )
        cfg = cast(PipelineConfig, DummyCfg())
        result = handlers._list_sections(
            cfg,
            args,
        )
        assert 'jobs' in result

    def test_materialize_csv_payload_non_str(self) -> None:
        """Test that non-string payloads return unchanged."""
        # pylint: disable=protected-access

        payload: object = {'foo': 1}
        assert handlers._materialize_csv_payload(payload) is payload

    def test_materialize_csv_payload_non_csv(self, tmp_path: Path) -> None:
        """Test that non-CSV file paths are returned unchanged."""
        # pylint: disable=protected-access

        file_path = tmp_path / 'file.txt'
        file_path.write_text('abc')
        assert handlers._materialize_csv_payload(str(file_path)) == str(
            file_path,
        )

    def test_materialize_csv_payload_csv(self, tmp_path: Path) -> None:
        """Test that CSV file paths are loaded into row dictionaries."""
        # pylint: disable=protected-access

        file_path = tmp_path / 'file.csv'
        file_path.write_text(CSV_TEXT)
        rows = handlers._materialize_csv_payload(str(file_path))

        assert isinstance(rows, list)
        assert rows[0] == {'a': '1', 'b': '2'}

    def test_pipeline_summary(self) -> None:
        """
        Test that :func:`_pipeline_summary` returns a mapping for a pipeline
        config.
        """
        # pylint: disable=protected-access

        cfg = cast(PipelineConfig, DummyCfg())
        summary = handlers._pipeline_summary(cfg)
        result: Mapping[str, object] = summary
        assert result['name'] == 'p1'
        assert result['version'] == 'v1'
        assert set(result) >= {'sources', 'targets', 'jobs'}

    def test_read_csv_rows(self, tmp_path: Path) -> None:
        """
        Test that :func:`_read_csv_rows` reads a CSV into row dictionaries.
        """
        # pylint: disable=protected-access

        file_path = tmp_path / 'data.csv'
        file_path.write_text(CSV_TEXT)
        assert handlers._read_csv_rows(file_path) == [
            {'a': '1', 'b': '2'},
            {'a': '3', 'b': '4'},
        ]

    def test_write_json_output_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that, when a file path is provided, JSON is written via
        :class:`File`.
        """
        # pylint: disable=protected-access

        data = {'x': 1}

        dummy_file = Mock()
        monkeypatch.setattr(handlers, 'File', lambda _p, _f: dummy_file)

        handlers._write_json_output(data, 'out.json', success_message='msg')
        dummy_file.write_json.assert_called_once_with(data)
