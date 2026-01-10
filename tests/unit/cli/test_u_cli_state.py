"""
:mod:`tests.unit.cli.test_u_cli_state` module.

Unit tests for :mod:`etlplus.cli.state`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
import typer
from typer.testing import Result

import etlplus
import etlplus.cli.handlers as handlers
import etlplus.cli.state as cli_state_module

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

InvokeCli = Callable[..., Result]


# SECTION: TESTS ============================================================ #


class TestInferResourceType:
    """Unit test suite for :func:`infer_resource_type`."""

    def test_file_path(self, tmp_path: Path) -> None:
        """
        Test that :func:`infer_resource_type` detects local files via
        extension parsing.
        """
        path = tmp_path / 'payload.csv'
        path.write_text('a,b\n1,2\n', encoding='utf-8')
        assert cli_state_module.infer_resource_type(str(path)) == 'file'

    def test_invalid_raises(self) -> None:
        """
        Unknown resources raise ``ValueError`` to surface helpful guidance.
        """
        with pytest.raises(ValueError):
            cli_state_module.infer_resource_type('unknown-resource')

    @pytest.mark.parametrize(
        ('raw', 'expected'),
        (
            ('-', 'file'),
            ('https://example.com/data.json', 'api'),
            ('postgres://user@host/db', 'database'),
        ),
    )
    def test_variants(
        self,
        raw: str,
        expected: str,
    ) -> None:
        """
        Test that :func:`infer_resource_type` classifies common resource
        inputs.
        """
        assert cli_state_module.infer_resource_type(raw) == expected


class TestOptionalChoice:
    """Unit test suite for :func:`optional_choice`."""

    @pytest.mark.parametrize(
        ('choice', 'expected'),
        ((None, None), ('json', 'json')),
    )
    def test_passthrough_and_validation(
        self,
        choice: str | None,
        expected: str | None,
    ) -> None:
        """
        Test that :func:`optional_choice` preserves ``None`` and normalizes
        valid values.
        """
        assert (
            cli_state_module.optional_choice(
                choice,
                {'json', 'csv'},
                label='format',
            )
            == expected
        )

    @pytest.mark.parametrize('invalid', ('yaml', 'parquet'))
    def test_rejects_invalid(self, invalid: str) -> None:
        """Test that invalid choices raise :class:`typer.BadParameter`."""
        with pytest.raises(typer.BadParameter):
            cli_state_module.optional_choice(invalid, {'json'}, label='format')


class TestCliExtractState:
    """Unit test suite of command-line state tests for ``extract``."""

    def test_marks_explicit_format(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that ``extract`` marks the data format as explicit when provided.
        """
        calls: dict[str, object] = {}

        def fake_extract(**kwargs: object) -> int:
            calls.update(kwargs)
            return 0

        monkeypatch.setattr(handlers, 'extract_handler', fake_extract)

        result = invoke_cli(
            'extract',
            '/path/to/file.csv',
            '--source-format',
            'csv',
        )

        assert result.exit_code == 0
        assert calls['source'] == '/path/to/file.csv'
        assert calls['format_hint'] == 'csv'
        assert calls['format_explicit'] is True

    def test_respects_root_flags(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that root flags propagate into the handler namespace for
        ``extract``.
        """
        calls: dict[str, object] = {}

        def fake_extract(**kwargs: object) -> int:
            calls.update(kwargs)
            return 0

        monkeypatch.setattr(handlers, 'extract_handler', fake_extract)

        result = invoke_cli(
            '--no-pretty',
            '--quiet',
            'extract',
            '--source-type',
            'api',
            'https://example.com/data.json',
        )

        assert result.exit_code == 0
        assert calls['source_type'] == 'api'
        assert calls['source'] == 'https://example.com/data.json'
        assert calls['pretty'] is False


class TestCliLoadState:
    """Unit test suite of command-line state tests for ``load``."""

    def test_defaults_json_format(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that ``load`` defaults to JSON and marks the data format as
        implicit.
        """
        calls: dict[str, object] = {}

        def fake_load(**kwargs: object) -> int:
            calls.update(kwargs)
            return 0

        monkeypatch.setattr(handlers, 'load_handler', fake_load)

        result = invoke_cli(
            'load',
            '--target-type',
            'file',
            '/path/to/out.json',
        )

        assert result.exit_code == 0
        assert calls['target'] == '/path/to/out.json'
        assert calls['target_format'] is None
        assert calls['format_explicit'] is False

    def test_marks_explicit_target_format(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that ``load`` marks the target data format as explicit when
        provided.
        """
        calls: dict[str, object] = {}

        def fake_load(**kwargs: object) -> int:
            calls.update(kwargs)
            return 0

        monkeypatch.setattr(handlers, 'load_handler', fake_load)

        result = invoke_cli(
            'load',
            '--target-format',
            'csv',
            '/path/to/out.csv',
        )

        assert result.exit_code == 0
        assert calls['target_format'] == 'csv'
        assert calls['format_explicit'] is True

    def test_defaults_source_to_stdin(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that ``load`` defaults to stdin when only target options are
        provided.
        """
        calls: dict[str, object] = {}

        def fake_load(**kwargs: object) -> int:
            calls.update(kwargs)
            return 0

        monkeypatch.setattr(handlers, 'load_handler', fake_load)

        result = invoke_cli(
            'load',
            '--target-type',
            'database',
            'postgres://db.example.org/app',
        )

        assert result.exit_code == 0
        assert calls['source'] == '-'
        assert calls['target'] == 'postgres://db.example.org/app'


class TestCliRenderState:
    """Unit test suite of command-line state tests for ``render``."""

    def test_maps_namespace(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``render`` maps options into the handler namespace."""
        calls: dict[str, object] = {}

        def fake_render(**kwargs: object) -> int:
            calls.update(kwargs)
            return 0

        monkeypatch.setattr(handlers, 'render_handler', fake_render)

        result = invoke_cli(
            'render',
            '--config',
            'pipeline.yml',
            '--table',
            'Customers',
            '--template',
            'ddl',
            '--output',
            'out.sql',
        )

        assert result.exit_code == 0
        assert calls['config'] == 'pipeline.yml'
        assert calls['table'] == 'Customers'
        assert calls['template'] == 'ddl'
        assert calls['output'] == 'out.sql'


class TestCliRunState:
    """Unit test suite of command-line state tests for ``run``."""

    def test_maps_flags(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that ``run`` maps job flags into the handler namespace.
        """
        calls: dict[str, object] = {}

        def fake_run(**kwargs: object) -> int:
            calls.update(kwargs)
            return 0

        monkeypatch.setattr(handlers, 'run_handler', fake_run)

        result = invoke_cli(
            'run',
            '--config',
            'p.yml',
            '--job',
            'j1',
        )
        assert result.exit_code == 0
        assert calls['config'] == 'p.yml'
        assert calls['job'] == 'j1'


class TestCliTransformState:
    """Unit test suite of command-line state tests for ``transform``."""

    def test_parses_operations_json(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that ``transform`` parses JSON operations passed via
        ``--operations``.
        """
        calls: dict[str, object] = {}

        def fake_transform(**kwargs: object) -> int:
            calls.update(kwargs)
            return 0

        monkeypatch.setattr(handlers, 'transform_handler', fake_transform)

        result = invoke_cli(
            'transform',
            '/path/to/file.json',
            '--operations',
            '{"select": ["id"]}',
        )

        assert result.exit_code == 0
        assert calls['source'] == '/path/to/file.json'
        assert isinstance(calls['operations'], dict)
        assert calls['operations'].get('select') == ['id']

    def test_respects_source_format(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that ``transform`` propagates ``--source-format`` into the
        namespace.
        """
        calls: dict[str, object] = {}

        def fake_transform(**kwargs: object) -> int:
            calls.update(kwargs)
            return 0

        monkeypatch.setattr(handlers, 'transform_handler', fake_transform)

        result = invoke_cli(
            'transform',
            '--source-format',
            'csv',
        )

        assert result.exit_code == 0
        assert calls['source_format'] == 'csv'


class TestCliValidateState:
    """Unit test suite of command-line state tests for ``validate``."""

    def test_parses_rules_json(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that ``validate`` parses JSON rules passed via ``--rules``.
        """
        calls: dict[str, object] = {}

        def fake_validate(**kwargs: object) -> int:
            calls.update(kwargs)
            return 0

        monkeypatch.setattr(handlers, 'validate_handler', fake_validate)

        result = invoke_cli(
            'validate',
            '/path/to/file.json',
            '--rules',
            '{"required": ["id"]}',
        )

        assert result.exit_code == 0
        assert calls['source'] == '/path/to/file.json'
        assert isinstance(calls['rules'], dict)
        assert calls['rules'].get('required') == ['id']

    def test_respects_source_format(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that ``validate`` propagates ``--source-format`` into the
        namespace.
        """
        calls: dict[str, object] = {}

        def fake_validate(**kwargs: object) -> int:
            calls.update(kwargs)
            return 0

        monkeypatch.setattr(handlers, 'validate_handler', fake_validate)

        result = invoke_cli(
            'validate',
            '--source-format',
            'csv',
        )

        assert result.exit_code == 0
        assert calls['source_format'] == 'csv'


class TestCliHelp:
    """Unit test suite of command-line state tests for help text."""

    def test_no_args_prints_help(self, invoke_cli: InvokeCli) -> None:
        """Test that running with no arguments prints help text."""
        result = invoke_cli()
        assert result.exit_code == 0
        assert 'ETLPlus' in result.stdout


class TestCliVersionFlag:
    """Unit test suite of command-line state tests for global flags."""

    def test_version_flag_exits_zero(self, invoke_cli: InvokeCli) -> None:
        """Test that command option ``--version`` exits successfully."""
        result = invoke_cli('--version')
        assert result.exit_code == 0
        assert etlplus.__version__ in result.stdout
