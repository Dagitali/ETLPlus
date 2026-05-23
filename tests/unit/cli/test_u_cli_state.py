"""
:mod:`tests.unit.cli.test_u_cli_state` module.

Unit tests for :mod:`etlplus.cli._commands._state`.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import nullcontext
from pathlib import Path
from typing import TypedDict

import pytest
import typer

import etlplus
import etlplus.cli._commands._state as cli_state_mod
import etlplus.cli._commands.extract as extract_mod
import etlplus.cli._commands.load as load_mod
import etlplus.cli._commands.render as render_mod
import etlplus.cli._commands.run as run_mod
import etlplus.cli._commands.transform as transform_mod
import etlplus.cli._commands.validate as validate_mod

from ...pytest_shared_support import CaptureHandler
from .conftest import InvokeCli
from .conftest import TyperContextFactory
from .conftest import assert_mapping_contains
from .conftest import strip_ansi

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


class _ResolveResourceTypeKwargs(TypedDict, total=False):
    """Typed kwargs container for :func:`resolve_resource_type` test cases."""

    explicit_type: str | None
    override_type: str | None
    value: str
    label: str
    conflict_error: str | None
    legacy_file_error: str | None


# SECTION: CONSTANTS ======================================================== #


COMMAND_STATE_CASES = (
    pytest.param(
        extract_mod,
        'extract_handler',
        ('extract', '/path/to/file.csv', '--source-format', 'csv'),
        {
            'source': '/path/to/file.csv',
            'source_format': 'csv',
            'format_explicit': True,
        },
        id='extract-file-format',
    ),
    pytest.param(
        extract_mod,
        'extract_handler',
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
    pytest.param(
        load_mod,
        'load_handler',
        ('load', '--target-type', 'file', '/path/to/out.json'),
        {
            'target': '/path/to/out.json',
            'target_format': None,
            'format_explicit': False,
        },
        id='load-file-target',
    ),
    pytest.param(
        load_mod,
        'load_handler',
        ('load', '--target-format', 'csv', '/path/to/out.csv'),
        {'target_format': 'csv', 'format_explicit': True},
        id='load-explicit-target-format',
    ),
    pytest.param(
        load_mod,
        'load_handler',
        ('load', '--target-type', 'database', 'postgres://db.example.org/app'),
        {'source': '-', 'target': 'postgres://db.example.org/app'},
        id='load-default-source',
    ),
    pytest.param(
        render_mod,
        'render_handler',
        (
            'render',
            '--config',
            'pipeline.yml',
            '--table',
            'Customers',
            '--template',
            'ddl',
            '--output',
            'out.sql',
        ),
        {
            'config': 'pipeline.yml',
            'table': 'Customers',
            'template': 'ddl',
            'output': 'out.sql',
        },
        id='render-table-ddl',
    ),
    pytest.param(
        run_mod,
        'run_handler',
        ('run', '--config', 'p.yml', '--job', 'j1'),
        {'config': 'p.yml', 'job': 'j1'},
        id='run-config-job',
    ),
    pytest.param(
        transform_mod,
        'transform_handler',
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
        transform_mod,
        'transform_handler',
        ('transform', '--source-format', 'csv'),
        {'source_format': 'csv'},
        id='transform-source-format',
    ),
    pytest.param(
        validate_mod,
        'validate_handler',
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
        validate_mod,
        'validate_handler',
        ('validate', '--source-format', 'csv'),
        {'source_format': 'csv'},
        id='validate-source-format',
    ),
)


# SECTION: TESTS ============================================================ #


class TestCliCommandState:
    """Unit test suite for command-line argument state mapping."""

    @pytest.mark.parametrize(
        ('argv', 'expected'),
        [
            pytest.param(
                ('validate', '--schema-format', 'jsonschema'),
                '--schema-format requires --schema',
                id='schema-format-without-schema',
            ),
            pytest.param(
                (
                    'validate',
                    'data.json',
                    '--schema',
                    'schema.json',
                    '--rules',
                    '{"id": {"required": true}}',
                ),
                'Use either --rules or --schema/--schema-format, not both.',
                id='rules-and-schema-conflict',
            ),
        ],
    )
    def test_rejects_invalid_schema_option_combinations(
        self,
        invoke_cli: InvokeCli,
        argv: tuple[str, ...],
        expected: str,
    ) -> None:
        """Test validate command rejects conflicting schema option families."""
        result = invoke_cli(*argv)

        assert result.exit_code != 0
        assert expected in strip_ansi(result.output)

    @pytest.mark.parametrize(
        ('handler_module', 'handler_name', 'argv', 'expected'),
        COMMAND_STATE_CASES,
    )
    def test_maps_namespace(
        self,
        invoke_cli: InvokeCli,
        capture_handler: CaptureHandler,
        handler_module: object,
        handler_name: str,
        argv: tuple[str, ...],
        expected: dict[str, object],
    ) -> None:
        """Test that CLI args map to handler parameters correctly."""
        calls = capture_handler(handler_module, handler_name)

        result = invoke_cli(*argv)

        assert result.exit_code == 0
        assert_mapping_contains(calls, expected)


class TestCliHelp:
    """Unit test suite of command-line state tests for help text."""

    @pytest.mark.parametrize(
        ('argv', 'expected'),
        [
            pytest.param(('--help',), 'init', id='root-help'),
            pytest.param((), 'ETLPlus', id='no-args'),
            pytest.param(('--version',), etlplus.__version__, id='version'),
        ],
    )
    def test_global_output_contains_expected_text(
        self,
        invoke_cli: InvokeCli,
        argv: tuple[str, ...],
        expected: str,
    ) -> None:
        """Global help-like entrypoints should emit stable plain-text content."""
        result = invoke_cli(*argv)

        assert result.exit_code == 0
        assert expected in strip_ansi(result.stdout)

    def test_init_help_prints_path_argument_and_force_option(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """Test that ``init --help`` preserves the documented CLI surface."""
        result = invoke_cli('init', '--help')
        stdout = strip_ansi(result.stdout)
        assert result.exit_code == 0
        assert 'PATH' in stdout
        assert '--force' in stdout


class TestInferResourceType:
    """Unit tests for :meth:`ResourceTypeResolver.infer`."""

    def test_invalid_raises(self) -> None:
        """
        Test that unknown resources raise ``ValueError`` to surface helpful
        guidance.
        """
        with pytest.raises(ValueError, match='Could not infer resource type'):
            cli_state_mod.ResourceTypeResolver.infer('unknown-resource')

    @pytest.mark.parametrize(
        ('raw', 'expected', 'touch_file'),
        [
            pytest.param('-', 'file', False, id='stdin'),
            pytest.param('payload.csv', 'file', True, id='local-file'),
            pytest.param('https://example.com/data.json', 'api', False, id='api-url'),
            pytest.param(
                'postgres://user@host/db',
                'database',
                False,
                id='database-url',
            ),
        ],
    )
    def test_variants(
        self,
        tmp_path: Path,
        raw: str,
        expected: str,
        touch_file: bool,
    ) -> None:
        """
        Test that :meth:`ResourceTypeResolver.infer` classifies common resource
        inputs.
        """
        value = raw
        if touch_file:
            path = tmp_path / raw
            path.write_text('a,b\n1,2\n', encoding='utf-8')
            value = str(path)

        assert cli_state_mod.ResourceTypeResolver.infer(value) == expected


class TestCliStateHelpers:
    """Unit tests for :mod:`etlplus.cli._commands._state` helper branches."""

    def test_ensure_state_initializes_missing_context_state(
        self,
        typer_ctx_factory: TyperContextFactory,
    ) -> None:
        """
        Test that non-state ``ctx.obj`` values are replaced with
        :class:`CliState`.
        """
        ctx = typer_ctx_factory()
        ctx.obj = {'unexpected': True}

        state = cli_state_mod.ensure_state(ctx)

        assert isinstance(state, cli_state_mod.CliState)
        assert ctx.obj is state

    def test_set_state_replaces_context_state(
        self,
        typer_ctx_factory: TyperContextFactory,
    ) -> None:
        """Test that explicit root flags replace the stored CLI state."""
        ctx = typer_ctx_factory()
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

    @pytest.mark.parametrize(
        ('value', 'setup'),
        [
            pytest.param(None, None, id='missing-value'),
            pytest.param(
                'invalid',
                lambda monkeypatch: monkeypatch.setattr(
                    cli_state_mod.ResourceTypeResolver,
                    'infer',
                    lambda _value: (_ for _ in ()).throw(ValueError('bad')),
                ),
                id='invalid-resource',
            ),
        ],
    )
    def test_infer_resource_type_soft_returns_none_for_non_fatal_inputs(
        self,
        monkeypatch: pytest.MonkeyPatch,
        value: str | None,
        setup: Callable[[pytest.MonkeyPatch], None] | None,
    ) -> None:
        """Soft inference should preserve ``None`` and swallow bad inputs."""
        if setup is not None:
            setup(monkeypatch)

        assert cli_state_mod.ResourceTypeResolver.infer_soft(value) is None

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

    @pytest.mark.parametrize(
        (
            'inferred',
            'expected_resolved',
            'expected_validated',
            'expected_logged',
        ),
        [
            pytest.param(
                None,
                None,
                [],
                {
                    'role': 'source',
                    'value': 'payload.json',
                    'resource_type': None,
                },
                id='soft-inference-miss',
            ),
            pytest.param(
                'file',
                'file',
                [('file', cli_state_mod.DATA_CONNECTORS, 'source_type')],
                {
                    'role': 'source',
                    'value': 'payload.json',
                    'resource_type': 'file',
                },
                id='soft-inference-hit',
            ),
        ],
    )
    def test_resolve_logged_resource_type_soft_inference(
        self,
        monkeypatch: pytest.MonkeyPatch,
        inferred: str | None,
        expected_resolved: str | None,
        expected_validated: list[tuple[object, object, str]],
        expected_logged: dict[str, object],
    ) -> None:
        """Soft inference should validate only resolved connector types."""
        logged: dict[str, object] = {}
        validated: list[tuple[object, object, str]] = []
        monkeypatch.setattr(
            cli_state_mod.ResourceTypeResolver,
            'infer_soft',
            lambda _value: inferred,
        )

        def validate_choice(
            value: object,
            choices: object,
            *,
            label: str,
        ) -> str:
            validated.append((value, choices, label))
            return str(value)

        monkeypatch.setattr(
            cli_state_mod.ResourceTypeResolver,
            'validate',
            validate_choice,
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

        assert resolved == expected_resolved
        assert validated == expected_validated
        assert logged == expected_logged

    @pytest.mark.parametrize(
        ('kwargs', 'infer_result', 'expected', 'expected_error'),
        [
            pytest.param(
                {
                    'explicit_type': 'api',
                    'override_type': 'file',
                    'value': 'input',
                    'label': 'source_type',
                    'conflict_error': 'conflict',
                },
                None,
                None,
                'conflict',
                id='conflict',
            ),
            pytest.param(
                {
                    'explicit_type': 'file',
                    'override_type': None,
                    'value': 'input',
                    'label': 'source_type',
                    'legacy_file_error': 'legacy',
                },
                None,
                None,
                'legacy',
                id='legacy-file',
            ),
            pytest.param(
                {
                    'explicit_type': 'api',
                    'override_type': None,
                    'value': 'input',
                    'label': 'source_type',
                    'legacy_file_error': 'legacy',
                },
                None,
                'api',
                None,
                id='explicit-non-file',
            ),
            pytest.param(
                {
                    'explicit_type': None,
                    'override_type': None,
                    'value': 'https://example.com/items',
                    'label': 'source_type',
                },
                'api',
                'api',
                None,
                id='inferred',
            ),
        ],
    )
    def test_resolve_resource_type(
        self,
        monkeypatch: pytest.MonkeyPatch,
        kwargs: _ResolveResourceTypeKwargs,
        infer_result: str | None,
        expected: str | None,
        expected_error: str | None,
    ) -> None:
        """Resource type resolution should honor precedence and validation rules."""
        if infer_result is not None:
            monkeypatch.setattr(
                cli_state_mod.ResourceTypeResolver,
                'infer_or_exit',
                lambda _value: infer_result,
            )

        if expected_error is not None:
            with pytest.raises(typer.BadParameter, match=expected_error):
                cli_state_mod.ResourceTypeResolver.resolve(**kwargs)
            return

        assert cli_state_mod.ResourceTypeResolver.resolve(**kwargs) == expected

    def test_resource_type_resolver_infer_soft_uses_function_seam(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that class-based soft inference still respects wrapper patches."""
        monkeypatch.setattr(
            cli_state_mod.ResourceTypeResolver,
            'infer',
            lambda _value: (_ for _ in ()).throw(ValueError('bad')),
        )

        assert cli_state_mod.ResourceTypeResolver.infer_soft('invalid') is None


class TestOptionalChoice:
    """Unit tests for :meth:`ResourceTypeResolver.optional_choice`."""

    @pytest.mark.parametrize(
        ('choice', 'choices', 'expected'),
        [
            pytest.param(None, {'json', 'csv'}, None, id='none'),
            pytest.param('json', {'json', 'csv'}, 'json', id='valid'),
            pytest.param('yaml', {'json'}, typer.BadParameter, id='yaml'),
            pytest.param('parquet', {'json'}, typer.BadParameter, id='parquet'),
        ],
    )
    def test_optional_choice(
        self,
        choice: str | None,
        choices: set[str],
        expected: str | type[Exception] | None,
    ) -> None:
        """
        Optional choice helpers should preserve ``None``, normalize valid
        values, and reject invalid choices.
        """
        expectation = (
            pytest.raises(expected)
            if isinstance(expected, type) and issubclass(expected, Exception)
            else nullcontext(expected)
        )

        with expectation as expected_value:
            assert (
                cli_state_mod.ResourceTypeResolver.optional_choice(
                    choice,
                    choices,
                    label='format',
                )
                == expected_value
            )

    @pytest.mark.parametrize(
        'invalid',
        [
            pytest.param(0, id='zero'),
            pytest.param(False, id='false'),
        ],
    )
    def test_validate_preserves_falsey_invalid_values(
        self,
        invalid: object,
    ) -> None:
        """Choice validation should report falsey invalid values faithfully."""
        with pytest.raises(typer.BadParameter, match=repr(invalid)):
            cli_state_mod.ResourceTypeResolver.validate(
                invalid,
                {'json'},
                label='format',
            )
