"""
:mod:`tests.unit.cli.test_u_cli_app` module.

Unit tests for :mod:`etlplus.cli.app`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner
from typer.testing import Result

import etlplus
import etlplus.cli.handlers as handlers
import etlplus.cli.state as cli_state_module
from etlplus.cli.commands import app as cli_app

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

InvokeCli = Callable[..., Result]


@pytest.fixture(name='invoke_cli')
def invoke_cli_fixture(
    runner: CliRunner,
) -> InvokeCli:
    """Invoke the Typer CLI."""

    def _invoke(*args: str) -> Result:
        return runner.invoke(cli_app, list(args))

    return _invoke


# SECTION: TESTS ============================================================ #


class TestCliAppInternalHelpers:
    """Unit tests for private helper utilities."""

    @pytest.mark.parametrize(
        ('raw', 'expected'),
        (
            ('-', 'file'),
            ('https://example.com/data.json', 'api'),
            ('postgres://user@host/db', 'database'),
        ),
    )
    def test_infer_resource_type_variants(
        self,
        raw: str,
        expected: str,
    ) -> None:
        """
        Test that :func:`_infer_resource_type` classifies common resource
        inputs.
        """
        assert cli_state_module.infer_resource_type(raw) == expected

    def test_infer_resource_type_file_path(self, tmp_path: Path) -> None:
        """
        Test that :func:`_infer_resource_type` detects local files via
        extension parsing.
        """
        path = tmp_path / 'payload.csv'
        path.write_text('a,b\n1,2\n', encoding='utf-8')
        assert cli_state_module.infer_resource_type(str(path)) == 'file'

    def test_infer_resource_type_invalid_raises(self) -> None:
        """
        Unknown resources raise ``ValueError`` to surface helpful guidance.
        """
        with pytest.raises(ValueError):
            cli_state_module.infer_resource_type('unknown-resource')

    @pytest.mark.parametrize(
        ('choice', 'expected'),
        ((None, None), ('json', 'json')),
    )
    def test_optional_choice_passthrough_and_validation(
        self,
        choice: str | None,
        expected: str | None,
    ) -> None:
        """
        Test that :func:`_optional_choice` preserves ``None`` and normalizes
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
    def test_optional_choice_rejects_invalid(self, invalid: str) -> None:
        """Test that invalid choices raise :class:`typer.BadParameter`."""
        with pytest.raises(typer.BadParameter):
            cli_state_module.optional_choice(invalid, {'json'}, label='format')

    def test_extract_explicit_format_maps_namespace(
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

    def test_extract_from_option_sets_source_type_and_state_flags(
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

    def test_load_default_format_maps_namespace(
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

    def test_load_explicit_format_maps_namespace(
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

    def test_load_to_option_defaults_source_to_stdin(
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

    def test_no_args_prints_help(self, runner: CliRunner) -> None:
        """Test invoking with no args prints help and exits 0."""
        result = runner.invoke(cli_app, [])
        assert result.exit_code == 0
        assert 'ETLPlus' in result.stdout

    def test_render_maps_namespace(
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

    def test_run_maps_flags(
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

    def test_transform_parses_operations_json(
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

    def test_transform_respects_source_format(
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

    def test_validate_parses_rules_json(
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

    def test_validate_respects_source_format(
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

    def test_version_flag_exits_zero(self, runner: CliRunner) -> None:
        """Test that command option ``--version`` exits successfully."""
        result = runner.invoke(cli_app, ['--version'])
        assert result.exit_code == 0
        assert etlplus.__version__ in result.stdout
