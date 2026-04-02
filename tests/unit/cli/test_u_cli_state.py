"""
:mod:`tests.unit.cli.test_u_cli_state` module.

Unit tests for :mod:`etlplus.cli._commands._state`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import typer

import etlplus
import etlplus.cli._commands as commands
import etlplus.cli._commands._state as cli_state_mod
import etlplus.cli._commands.extract as extract_mod
import etlplus.cli._commands.load as load_mod
import etlplus.cli._commands.render as render_mod
import etlplus.cli._commands.run as run_mod
import etlplus.cli._commands.transform as transform_mod
import etlplus.cli._commands.validate as validate_mod

from ...conftest import CaptureHandler
from .conftest import InvokeCli
from .conftest import assert_mapping_contains

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestCliExtractState:
    """Unit test suite of command-line state tests for ``extract``."""

    @pytest.mark.parametrize(
        ('argv', 'expected'),
        [
            pytest.param(
                ('extract', '/path/to/file.csv', '--source-format', 'csv'),
                {
                    'source': '/path/to/file.csv',
                    'source_format': 'csv',
                    'format_explicit': True,
                },
                id='extract-file-format',
            ),
            pytest.param(
                (
                    '--no-pretty',
                    '--quiet',
                    'extract',
                    '--source-type',
                    'api',
                    'https://example.com/data.json',
                ),
                {
                    'source_type': 'api',
                    'source': 'https://example.com/data.json',
                    'pretty': False,
                },
                id='extract-api-quiet',
            ),
        ],
    )
    def test_maps_namespace(
        self,
        invoke_cli: InvokeCli,
        capture_handler: CaptureHandler,
        argv: tuple[str, ...],
        expected: dict[str, object],
    ) -> None:
        """Test that CLI args map to handler parameters correctly."""
        calls = capture_handler(extract_mod, 'extract_handler')

        result = invoke_cli(*argv)

        assert result.exit_code == 0
        assert_mapping_contains(calls, expected)


class TestCliLoadState:
    """Unit test suite of command-line state tests for ``load``."""

    @pytest.mark.parametrize(
        ('argv', 'expected'),
        [
            pytest.param(
                ('load', '--target-type', 'file', '/path/to/out.json'),
                {
                    'target': '/path/to/out.json',
                    'target_format': None,
                    'format_explicit': False,
                },
                id='load-file-target',
            ),
            pytest.param(
                ('load', '--target-format', 'csv', '/path/to/out.csv'),
                {'target_format': 'csv', 'format_explicit': True},
                id='load-explicit-target-format',
            ),
            pytest.param(
                (
                    'load',
                    '--target-type',
                    'database',
                    'postgres://db.example.org/app',
                ),
                {'source': '-', 'target': 'postgres://db.example.org/app'},
                id='load-default-source',
            ),
        ],
    )
    def test_maps_namespace(
        self,
        invoke_cli: InvokeCli,
        capture_handler: CaptureHandler,
        argv: tuple[str, ...],
        expected: dict[str, object],
    ) -> None:
        """Test that CLI args map to handler parameters correctly."""
        calls = capture_handler(load_mod, 'load_handler')

        result = invoke_cli(*argv)

        assert result.exit_code == 0
        assert_mapping_contains(calls, expected)


class TestCliRenderState:
    """Unit test suite of command-line state tests for ``render``."""

    def test_maps_namespace(
        self,
        invoke_cli: InvokeCli,
        capture_handler: CaptureHandler,
    ) -> None:
        """Test that CLI args map to handler parameters correctly."""
        calls = capture_handler(render_mod, 'render_handler')

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
        capture_handler: CaptureHandler,
    ) -> None:
        """Test that CLI flags map to handler parameters correctly."""
        calls = capture_handler(run_mod, 'run_handler')

        result = invoke_cli('run', '--config', 'p.yml', '--job', 'j1')

        assert result.exit_code == 0
        assert calls['config'] == 'p.yml'
        assert calls['job'] == 'j1'


class TestCliTransformState:
    """Unit test suite of command-line state tests for ``transform``."""

    @pytest.mark.parametrize(
        ('argv', 'expected'),
        [
            pytest.param(
                (
                    'transform',
                    '/path/to/file.json',
                    '--operations',
                    '{"select": ["id"]}',
                ),
                {
                    'source': '/path/to/file.json',
                    'operations': {'select': ['id']},
                },
                id='transform-inline-ops',
            ),
            pytest.param(
                ('transform', '--source-format', 'csv'),
                {'source_format': 'csv'},
                id='transform-source-format',
            ),
        ],
    )
    def test_maps_namespace(
        self,
        invoke_cli: InvokeCli,
        capture_handler: CaptureHandler,
        argv: tuple[str, ...],
        expected: dict[str, object],
    ) -> None:
        """Test that CLI args map to handler parameters correctly."""
        calls = capture_handler(transform_mod, 'transform_handler')

        result = invoke_cli(*argv)

        assert result.exit_code == 0
        assert_mapping_contains(calls, expected)


class TestCliValidateState:
    """Unit test suite of command-line state tests for ``validate``."""

    @pytest.mark.parametrize(
        ('argv', 'expected'),
        [
            pytest.param(
                (
                    'validate',
                    '/path/to/file.json',
                    '--rules',
                    '{"required": ["id"]}',
                ),
                {
                    'source': '/path/to/file.json',
                    'rules': {'required': ['id']},
                },
                id='validate-inline-rules',
            ),
            pytest.param(
                ('validate', '--source-format', 'csv'),
                {'source_format': 'csv'},
                id='validate-source-format',
            ),
        ],
    )
    def test_maps_namespace(
        self,
        invoke_cli: InvokeCli,
        capture_handler: CaptureHandler,
        argv: tuple[str, ...],
        expected: dict[str, object],
    ) -> None:
        """Test that CLI args map to handler parameters correctly."""
        calls = capture_handler(validate_mod, 'validate_handler')

        result = invoke_cli(*argv)

        assert result.exit_code == 0
        assert_mapping_contains(calls, expected)


class TestCliHelp:
    """Unit test suite of command-line state tests for help text."""

    def test_help_flag_prints_help(self, invoke_cli: InvokeCli) -> None:
        """Test that the global ``--help`` flag builds the full command tree."""
        result = invoke_cli('--help')
        assert result.exit_code == 0
        assert 'init' in result.stdout

    def test_init_help_prints_path_argument_and_force_option(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """Test that ``init --help`` preserves the documented CLI surface."""
        result = invoke_cli('init', '--help')
        assert result.exit_code == 0
        assert 'PATH' in result.stdout
        assert '--force' in result.stdout

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


class TestInferResourceType:
    """Unit tests for :func:`infer_resource_type`."""

    def test_file_path(self, tmp_path: Path) -> None:
        """
        Test that :func:`infer_resource_type` detects local files via extension
        parsing.
        """
        path = tmp_path / 'payload.csv'
        path.write_text('a,b\n1,2\n', encoding='utf-8')
        assert cli_state_mod.infer_resource_type(str(path)) == 'file'

    def test_invalid_raises(self) -> None:
        """
        Test that unknown resources raise ``ValueError`` to surface helpful
        guidance.
        """
        with pytest.raises(ValueError, match='Could not infer resource type'):
            cli_state_mod.infer_resource_type('unknown-resource')

    @pytest.mark.parametrize(
        ('raw', 'expected'),
        [
            ('-', 'file'),
            ('https://example.com/data.json', 'api'),
            ('postgres://user@host/db', 'database'),
        ],
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
        assert cli_state_mod.infer_resource_type(raw) == expected


class TestCliStateHelpers:
    """Unit tests for :mod:`etlplus.cli._commands._state` helper branches."""

    def test_ensure_state_initializes_missing_context_state(self) -> None:
        """
        Test that non-state ``ctx.obj`` values are replaced with
        :class:`CliState`.
        """
        command = typer.main.get_command(commands.app)
        ctx = typer.Context(command)
        ctx.obj = {'unexpected': True}

        state = cli_state_mod.ensure_state(ctx)

        assert isinstance(state, cli_state_mod.CliState)
        assert ctx.obj is state

    def test_set_state_replaces_context_state(self) -> None:
        """Test that explicit root flags replace the stored CLI state."""
        command = typer.main.get_command(commands.app)
        ctx = typer.Context(command)
        ctx.obj = {'unexpected': True}

        state = cli_state_mod._set_state(
            ctx,
            pretty=False,
            quiet=True,
            verbose=True,
        )

        assert state == cli_state_mod.CliState(
            pretty=False,
            quiet=True,
            verbose=True,
        )
        assert ctx.obj is state

    def test_infer_resource_type_soft_none_returns_none(self) -> None:
        """Test that soft inference returns ``None`` for missing values."""
        assert cli_state_mod.infer_resource_type_soft(None) is None

    def test_infer_resource_type_soft_swallows_inference_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that soft inference returns ``None`` for invalid resources."""
        monkeypatch.setattr(
            cli_state_mod,
            'infer_resource_type',
            lambda _value: (_ for _ in ()).throw(ValueError('bad')),
        )
        assert cli_state_mod.infer_resource_type_soft('invalid') is None

    def test_log_inferred_resource_prints_verbose_messages(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that verbose mode emits inferred-resource diagnostics."""
        cli_state_mod.log_inferred_resource(
            cli_state_mod.CliState(pretty=True, quiet=False, verbose=True),
            role='source',
            value='input.json',
            resource_type='file',
        )
        assert 'Inferred source_type=file' in capsys.readouterr().err

    def test_resolve_logged_resource_type_returns_none_when_soft_inference_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Soft inference should allow a ``None`` result without validation."""
        logged: dict[str, object] = {}
        validated: list[object] = []
        monkeypatch.setattr(
            cli_state_mod,
            'infer_resource_type_soft',
            lambda _value: None,
        )
        monkeypatch.setattr(
            cli_state_mod,
            'validate_choice',
            lambda value, choices, *, label: validated.append((value, choices, label)),
        )
        monkeypatch.setattr(
            cli_state_mod,
            'log_inferred_resource',
            lambda state, **kwargs: logged.update(kwargs),
        )

        resolved = cli_state_mod.resolve_logged_resource_type(
            cli_state_mod.CliState(verbose=True),
            role='source',
            value='payload.json',
            explicit_type=None,
            soft_inference=True,
        )

        assert resolved is None
        assert validated == []
        assert logged == {
            'role': 'source',
            'value': 'payload.json',
            'resource_type': None,
        }

    def test_resolve_logged_resource_type_uses_soft_inference(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test the shared resolve/validate/log state helper."""
        logged: dict[str, object] = {}
        monkeypatch.setattr(
            cli_state_mod,
            'infer_resource_type_soft',
            lambda _value: 'file',
        )
        monkeypatch.setattr(
            cli_state_mod,
            'log_inferred_resource',
            lambda state, **kwargs: logged.update(kwargs),
        )

        resolved = cli_state_mod.resolve_logged_resource_type(
            cli_state_mod.CliState(verbose=True),
            role='source',
            value='payload.json',
            explicit_type=None,
            soft_inference=True,
        )

        assert resolved == 'file'
        assert logged == {
            'role': 'source',
            'value': 'payload.json',
            'resource_type': 'file',
        }

    def test_resolve_resource_type_conflict_raises_bad_parameter(self) -> None:
        """Test that conflicting explicit/override values raise errors."""
        with pytest.raises(typer.BadParameter, match='conflict'):
            cli_state_mod.resolve_resource_type(
                explicit_type='api',
                override_type='file',
                value='input',
                label='source_type',
                conflict_error='conflict',
            )

    def test_resolve_resource_type_legacy_file_raises_bad_parameter(
        self,
    ) -> None:
        """
        Test that legacy file-specific explicit type raises when disallowed.
        """
        with pytest.raises(typer.BadParameter, match='legacy'):
            cli_state_mod.resolve_resource_type(
                explicit_type='file',
                override_type=None,
                value='input',
                label='source_type',
                legacy_file_error='legacy',
            )

    def test_resolve_resource_type_accepts_explicit_non_file_value(
        self,
    ) -> None:
        """Test that explicit non-file values pass through validation."""
        resolved = cli_state_mod.resolve_resource_type(
            explicit_type='api',
            override_type=None,
            value='input',
            label='source_type',
            legacy_file_error='legacy',
        )
        assert resolved == 'api'

    def test_resolve_resource_type_infers_when_no_explicit_or_override(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Resolver should infer a connector type when no preference is given."""
        monkeypatch.setattr(
            cli_state_mod,
            'infer_resource_type_or_exit',
            lambda _value: 'api',
        )

        resolved = cli_state_mod.resolve_resource_type(
            explicit_type=None,
            override_type=None,
            value='https://example.com/items',
            label='source_type',
        )

        assert resolved == 'api'

    def test_resource_type_resolver_infer_soft_uses_function_seam(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that class-based soft inference still respects wrapper patches."""
        monkeypatch.setattr(
            cli_state_mod,
            'infer_resource_type',
            lambda _value: (_ for _ in ()).throw(ValueError('bad')),
        )

        assert cli_state_mod.ResourceTypeResolver.infer_soft('invalid') is None


class TestOptionalChoice:
    """Unit tests for :func:`optional_choice`."""

    @pytest.mark.parametrize(
        ('choice', 'expected'),
        [(None, None), ('json', 'json')],
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
            cli_state_mod.optional_choice(
                choice,
                {'json', 'csv'},
                label='format',
            )
            == expected
        )

    @pytest.mark.parametrize('invalid', ['yaml', 'parquet'])
    def test_rejects_invalid(self, invalid: str) -> None:
        """Test that invalid choices raise :class:`typer.BadParameter`."""
        with pytest.raises(typer.BadParameter):
            cli_state_mod.optional_choice(invalid, {'json'}, label='format')

    def test_resource_type_resolver_optional_choice_preserves_none(self) -> None:
        """Test that class-based optional choice preserves missing values."""
        assert (
            cli_state_mod.ResourceTypeResolver.optional_choice(
                None,
                {'json', 'csv'},
                label='format',
            )
            is None
        )
